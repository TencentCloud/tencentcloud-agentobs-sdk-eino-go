package agentobs

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type (
	agentScopeKey struct{}
	stepOwnerKey  struct{}
)

// agentScope is created for every agent span. It owns the synthesized ReAct
// step spans: each model call inside the agent opens a new step, and tool
// calls requested by that model attach to the same step.
type agentScope struct {
	h     *Handler
	state *spanState
	// parent is the enclosing agent of a subagent.
	parent *agentScope
	id     string
	name   string
	turn   *turn
	run    *traceRun
	source string

	mu              sync.Mutex
	round           int
	current         *stepSpan
	stepsByToolCall map[string]*stepSpan
	// onIdle ends the agent span once pendingChats drops to zero.
	onIdle func()
	// tokens sums the usage of model calls in this agent's subtree,
	// including subagents.
	tokens       tokenTotals
	pendingChats int
	finished     bool

	toolCalls    atomic.Int64
	messageCount atomic.Int64
}

// beginStep closes the current step and opens the next ReAct round.
func (a *agentScope) beginStep() *stepSpan {
	a.mu.Lock()
	if a.finished {
		a.mu.Unlock()
		return nil
	}
	previous := a.current
	a.round++
	step := &stepSpan{scope: a, round: a.round, id: a.turn.nextStepID()}
	a.current = step
	a.mu.Unlock()

	if previous != nil {
		previous.requestClose()
	}
	if a.h.stepSpans {
		step.start()
	}
	return step
}

// stepForToolCall returns the step whose model requested toolCallID, or the
// current step.
func (a *agentScope) stepForToolCall(toolCallID string) *stepSpan {
	a.mu.Lock()
	defer a.mu.Unlock()
	if toolCallID != "" {
		if step, ok := a.stepsByToolCall[toolCallID]; ok {
			return step
		}
	}
	return a.current
}

func (a *agentScope) bindToolCalls(step *stepSpan, ids []string) {
	if step == nil || len(ids) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stepsByToolCall == nil {
		a.stepsByToolCall = make(map[string]*stepSpan, len(ids))
	}
	for _, id := range ids {
		a.stepsByToolCall[id] = step
	}
}

// chatStarted registers a model call in this agent's subtree.
func (a *agentScope) chatStarted() {
	a.mu.Lock()
	a.pendingChats++
	a.mu.Unlock()
}

// chatEnded adds a finished model call's usage and ends the agent span if it
// was only waiting for this call.
func (a *agentScope) chatEnded(usage tokenTotals) {
	a.mu.Lock()
	a.tokens.add(usage)
	if a.pendingChats > 0 {
		a.pendingChats--
	}
	var end func()
	if a.pendingChats == 0 {
		end, a.onIdle = a.onIdle, nil
	}
	a.mu.Unlock()
	if end != nil {
		end()
	}
}

// deferEnd postpones end until running model calls in the subtree finish. It
// reports false if nothing is running and the caller should end now.
func (a *agentScope) deferEnd(end func()) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pendingChats == 0 {
		return false
	}
	a.onIdle = end
	return true
}

// finish writes agent aggregates and closes the last step. It runs just before
// the agent span ends.
func (a *agentScope) finish() {
	a.mu.Lock()
	if a.finished {
		a.mu.Unlock()
		return
	}
	a.finished = true
	current, tokens := a.current, a.tokens
	a.mu.Unlock()

	a.state.attrs.setInt(attrAgentToolCallCount, int(a.toolCalls.Load()))
	if count := a.messageCount.Load(); count > 0 || a.source == agentSourceADK {
		a.state.attrs.setInt(attrAgentMessageCount, int(count))
	}
	a.state.attrs.setInt(attrUsageInputTokens, tokens.input)
	a.state.attrs.setInt(attrUsageOutputTokens, tokens.output)
	a.state.attrs.setInt(attrUsageTotalTokens, tokens.total)
	a.state.attrs.setInt(attrUsageCacheReadTokens, tokens.cacheRead)
	if current != nil {
		current.requestClose()
	}
}

// stepSpan is a synthesized ReAct round. It ends once it was superseded (or
// its agent finished) and all chat/tool children have ended.
type stepSpan struct {
	scope *agentScope
	round int
	id    string

	span   trace.Span
	attrs  *spanAttributeWriter
	run    *traceRun
	failed bool

	mu             sync.Mutex
	active         int
	closeRequested bool
	ended          bool
	finishReason   string
}

func (s *stepSpan) start() {
	a := s.scope
	parent := trace.ContextWithSpan(context.Background(), a.state.span)
	_, span := a.h.tracer.Start(parent, "react round_"+strconv.Itoa(s.round), trace.WithSpanKind(trace.SpanKindInternal))
	s.span = span
	s.attrs = newSpanAttributeWriter(span, a.h.prepare, a.h.maxSpanAttributeBytes, a.h.losses)
	s.attrs.setString(attrSpanKind, SpanKindStep)
	s.attrs.setString(attrOperationName, OperationReact)
	a.turn.apply(s.attrs)
	s.attrs.setString(attrAgentID, a.id)
	s.attrs.setString(attrAgentName, a.name)
	s.attrs.setString(attrStepID, s.id)
	s.attrs.setInt(attrReactRound, s.round)
	if a.run != nil && a.run.childStarted() {
		s.run = a.run
	}
}

func (s *stepSpan) parentContext(ctx context.Context) context.Context {
	if s == nil || s.span == nil {
		return ctx
	}
	return trace.ContextWithSpan(ctx, s.span)
}

func (s *stepSpan) applyTo(w *spanAttributeWriter) {
	if s == nil {
		return
	}
	w.setString(attrStepID, s.id)
	w.setInt(attrReactRound, s.round)
}

func (s *stepSpan) childStarted() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.active++
	s.mu.Unlock()
}

func (s *stepSpan) childEnded() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.active > 0 {
		s.active--
	}
	shouldEnd := s.closeRequested && s.active == 0 && !s.ended
	if shouldEnd {
		s.ended = true
	}
	s.mu.Unlock()
	if shouldEnd {
		s.end()
	}
}

func (s *stepSpan) requestClose() {
	s.mu.Lock()
	s.closeRequested = true
	shouldEnd := s.active == 0 && !s.ended
	if shouldEnd {
		s.ended = true
	}
	s.mu.Unlock()
	if shouldEnd {
		s.end()
	}
}

func (s *stepSpan) setFinishReason(reason string) {
	if s == nil || reason == "" {
		return
	}
	s.mu.Lock()
	s.finishReason = reason
	s.mu.Unlock()
}

func (s *stepSpan) recordModelError(message string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.failed = true
	s.mu.Unlock()
	if s.span != nil {
		s.span.SetStatus(codes.Error, message)
		s.attrs.setString(attrErrorType, "chat_error")
	}
}

func (s *stepSpan) end() {
	if s.span == nil {
		return
	}
	s.mu.Lock()
	reason, failed := s.finishReason, s.failed
	s.mu.Unlock()
	if failed {
		reason = finishReasonError
	}
	s.attrs.setString(attrReactFinishReason, reason)
	if !failed {
		s.span.SetStatus(codes.Ok, "")
	}
	s.span.End(trace.WithTimestamp(time.Now()))
	if s.run != nil {
		s.run.childEnded("", false)
	}
}
