// This file contains code adapted from github.com/cloudwego/eino-ext
// (callbacks/langfuse/v2), Copyright 2026 CloudWeGo Authors, licensed under
// the Apache License, Version 2.0. It was modified to emit Tencent Cloud CLS
// Agent Trace spans over OTLP/HTTP instead of Langfuse observations.

package agentobs

import (
	"context"
	"crypto/rand"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type (
	desiredTraceIDKey struct{}
	traceRunKey       struct{}
	turnKey           struct{}
)

type traceOptions struct {
	Attributes    map[string]string
	TraceID       string
	Name          string
	SessionID     string
	TurnID        string
	UserID        string
	UserName      string
	AgentType     string
	EntryType     string
	Input         string
	InputMessages []*schema.Message
	TurnIndex     int
}

// TraceOption configures StartTrace.
type TraceOption func(*traceOptions)

// WithTraceID requests a specific 32-hex-character trace ID.
func WithTraceID(id string) TraceOption { return func(o *traceOptions) { o.TraceID = id } }

// WithName overrides the entry span name (default "enter_application").
func WithName(name string) TraceOption { return func(o *traceOptions) { o.Name = name } }

// WithSessionID sets gen_ai.session.id. Defaults to the trace ID.
func WithSessionID(id string) TraceOption { return func(o *traceOptions) { o.SessionID = id } }

// WithTurnID sets gen_ai.turn.id verbatim.
func WithTurnID(id string) TraceOption { return func(o *traceOptions) { o.TurnID = id } }

// WithTurnIndex derives gen_ai.turn.id as "<session>:t<index>", matching the
// CLS ID hierarchy. Use a value the caller persists per session (for example
// the number of user messages); it is ignored when WithTurnID is set.
func WithTurnIndex(index int) TraceOption { return func(o *traceOptions) { o.TurnIndex = index } }

// WithUserID sets gen_ai.user.id.
func WithUserID(id string) TraceOption { return func(o *traceOptions) { o.UserID = id } }

// WithUserName sets gen_ai.user.name.
func WithUserName(name string) TraceOption { return func(o *traceOptions) { o.UserName = name } }

// WithAgentType overrides gen_ai.agent.type for this trace.
func WithAgentType(agentType string) TraceOption {
	return func(o *traceOptions) { o.AgentType = agentType }
}

// WithEntryType sets gen_ai.entry.type, for example "api", "cli" or "im".
func WithEntryType(entryType string) TraceOption {
	return func(o *traceOptions) { o.EntryType = entryType }
}

// WithInput records the user request text on the entry span.
func WithInput(input string) TraceOption { return func(o *traceOptions) { o.Input = input } }

// WithInputMessages records structured input messages on the entry span.
func WithInputMessages(messages ...*schema.Message) TraceOption {
	return func(o *traceOptions) { o.InputMessages = append(o.InputMessages, messages...) }
}

// WithAttributes adds custom string attributes (for example git.repo) to every
// span of the trace. Keys in the SDK-managed gen_ai.* and eino.* namespaces
// are ignored.
func WithAttributes(attributes map[string]string) TraceOption {
	return func(o *traceOptions) {
		if o.Attributes == nil {
			o.Attributes = make(map[string]string, len(attributes))
		}
		for key, value := range attributes {
			o.Attributes[key] = value
		}
	}
}

// turn holds the identity shared by every span of one trace and allocates
// step identifiers.
type turn struct {
	sessionID string
	turnID    string
	userID    string
	userName  string
	agentType string
	extra     []attribute.KeyValue
	stepSeq   atomic.Int64
}

func newTurn(options *traceOptions, traceID trace.TraceID) *turn {
	traceHex := traceID.String()
	session := defaultString(options.SessionID, traceHex)
	turnID := options.TurnID
	switch {
	case turnID != "":
	case options.TurnIndex > 0:
		turnID = session + ":t" + strconv.Itoa(options.TurnIndex)
	case session == traceHex:
		turnID = traceHex
	default:
		turnID = session + ":" + traceHex
	}
	t := &turn{
		sessionID: session,
		turnID:    turnID,
		userID:    options.UserID,
		userName:  options.UserName,
		agentType: defaultString(options.AgentType, defaultAgentType),
	}
	keys := make([]string, 0, len(options.Attributes))
	for key := range options.Attributes {
		key = strings.TrimSpace(key)
		if key == "" || strings.HasPrefix(key, "gen_ai.") || strings.HasPrefix(key, "eino.") {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if value := options.Attributes[key]; value != "" {
			t.extra = append(t.extra, attribute.String(key, value))
		}
	}
	return t
}

func (t *turn) apply(w *spanAttributeWriter) {
	if t == nil {
		return
	}
	w.setString(attrAgentType, t.agentType)
	w.setString(attrSessionID, t.sessionID)
	w.setString(attrTurnID, t.turnID)
	w.setString(attrUserID, t.userID)
	w.setString(attrUserName, t.userName)
	w.setAttributes(t.extra)
}

func (t *turn) nextStepID() string {
	return t.turnID + ":s" + strconv.FormatInt(t.stepSeq.Add(1), 10)
}

// StartTrace creates the CLS entry span for one user request (turn). Run Eino
// components with the returned context. The entry span ends on EndTrace, when
// ctx is done and every child callback has finished, or on Shutdown.
func (h *Handler) StartTrace(ctx context.Context, opts ...TraceOption) context.Context {
	options := h.defaults
	options.Attributes = nil
	options.InputMessages = nil
	for _, opt := range opts {
		opt(&options)
	}

	traceID := requestedTraceID(ctx, options.TraceID)
	rootCtx := trace.ContextWithSpanContext(ctx, trace.SpanContext{})
	if traceID.IsValid() {
		rootCtx = context.WithValue(rootCtx, desiredTraceIDKey{}, traceID)
	}
	rootCtx, span := h.tracer.Start(rootCtx, defaultString(options.Name, OperationEnterApplication),
		trace.WithSpanKind(trace.SpanKindServer))
	tn := newTurn(&options, span.SpanContext().TraceID())
	writer := newSpanAttributeWriter(span, h.prepare, h.maxSpanAttributeBytes, h.losses)
	writer.setString(attrSpanKind, SpanKindEntry)
	writer.setString(attrOperationName, OperationEnterApplication)
	tn.apply(writer)
	writer.setString(attrEntryType, defaultString(options.EntryType, defaultEntryType))

	run := &traceRun{span: span, writer: writer, turn: tn, done: make(chan struct{})}
	if h.captureContent {
		var input []genAIMessage
		if len(options.InputMessages) > 0 {
			input = convertMessages(options.InputMessages)
		} else if options.Input != "" {
			input = []genAIMessage{textMessage("user", options.Input)}
		}
		if len(input) > 0 {
			run.offerInput(h.prepareJSON(input))
		}
	}
	run.onEnd = func() { h.activeRuns.delete(run) }
	h.activeRuns.add(run)
	run.watchContext(ctx)

	rootCtx = context.WithValue(rootCtx, traceRunKey{}, run)
	return context.WithValue(rootCtx, turnKey{}, tn)
}

// EndTrace records the final answer on the entry span and ends it once all
// active child callbacks have finished. Calling it more than once is safe;
// the first non-empty output is kept.
func (h *Handler) EndTrace(ctx context.Context, output string) {
	run, _ := ctx.Value(traceRunKey{}).(*traceRun)
	if run == nil {
		return
	}
	var serialized string
	if output != "" && h.captureContent {
		serialized = h.prepareJSON([]genAIMessage{textMessage("assistant", output)})
	}
	run.requestEnd(serialized, true)
}

// traceRun owns the entry span. It waits for active children before ending
// and chooses the root output: explicit EndTrace output, then the outermost
// agent result, then the last chat output.
type traceRun struct {
	span           trace.Span
	writer         *spanAttributeWriter
	turn           *turn
	onEnd          func()
	done           chan struct{}
	agentOutput    string
	lastOutput     string
	explicitOutput string
	active         int
	mu             sync.Mutex
	hasAgentOutput bool
	endRequested   bool
	// endExplicit is set by EndTrace. A context that ends afterwards (for
	// example an HTTP request finishing while children are still flushing)
	// no longer changes the trace outcome.
	endExplicit bool
	hasInput    bool
	ended       bool
	failed      bool
	soft        bool
	autoEnd     bool
}

func (r *traceRun) childStarted() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended || r.endRequested {
		return false
	}
	r.active++
	return true
}

func (r *traceRun) childEnded(output string, outermostAgent bool) {
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return
	}
	if outermostAgent {
		r.agentOutput = output
		r.hasAgentOutput = true
	} else if output != "" {
		r.lastOutput = output
	}
	if r.active > 0 {
		r.active--
	}
	shouldEnd := r.active == 0 && (r.endRequested || r.autoEnd)
	r.mu.Unlock()
	if shouldEnd {
		r.end()
	}
}

// offerInput records the first observed input messages on the entry span.
func (r *traceRun) offerInput(serialized string) {
	if serialized == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended || r.hasInput {
		return
	}
	r.hasInput = true
	r.writer.setString(attrInputMessages, serialized)
	r.writer.setString(attrInputMessagesHash, contentHash(serialized))
}

func (r *traceRun) watchContext(ctx context.Context) {
	if ctx.Done() == nil {
		return
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = recoveredPanic("trace context watcher", recovered)
			}
		}()
		select {
		case <-ctx.Done():
			r.contextEnded(ctx)
		case <-r.done:
		}
	}()
}

func (r *traceRun) contextEnded(ctx context.Context) {
	r.mu.Lock()
	settled := r.ended || r.endExplicit
	r.mu.Unlock()
	if settled {
		return
	}
	err := ctx.Err()
	cause := context.Cause(ctx)
	if cause == nil {
		cause = err
	}
	if causeText, interrupted := interruptionCause(cause); interrupted {
		r.recordSoftStatus(statusInterrupted, causeText)
		r.requestEnd("", false)
		return
	}
	if errors.Is(err, context.Canceled) {
		r.recordSoftStatus(statusCancelled, cause.Error())
		r.requestEnd("", false)
		return
	}
	r.recordError("trace context: "+cause.Error(), cause)
	r.requestEnd("", false)
}

func (r *traceRun) requestEnd(output string, explicit bool) {
	r.mu.Lock()
	if r.ended {
		r.mu.Unlock()
		return
	}
	r.endRequested = true
	if explicit {
		r.endExplicit = true
	}
	if explicit && output != "" && r.explicitOutput == "" {
		r.explicitOutput = output
	}
	shouldEnd := r.active == 0
	r.mu.Unlock()
	if shouldEnd {
		r.end()
	}
}

func (r *traceRun) recordError(message string, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended || r.failed {
		return
	}
	r.failed = true
	r.span.RecordError(err)
	r.span.SetStatus(codes.Error, message)
	r.writer.setString(attrErrorType, errorType(SpanKindEntry, err))
	r.writer.setContent(attrErrorMessage, message)
}

func (r *traceRun) recordSoftStatus(status, cause string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ended {
		return
	}
	r.soft = true
	r.writer.setString(attrEinoStatus, status)
	r.writer.setContent(attrEinoStatusCause, cause)
}

func (r *traceRun) recordCallbackPanic(message string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.ended {
		r.writer.setString(attrEinoCallbackPanic, message)
	}
}

func (r *traceRun) end() {
	r.mu.Lock()
	if r.ended {
		done := r.done
		r.mu.Unlock()
		<-done
		return
	}
	r.ended = true
	var output string
	switch {
	case r.explicitOutput != "":
		output = r.explicitOutput
	case r.hasAgentOutput:
		output = r.agentOutput
	default:
		output = r.lastOutput
	}
	failed, soft := r.failed, r.soft
	onEnd := r.onEnd
	r.mu.Unlock()

	defer close(r.done)
	if onEnd != nil {
		defer onEnd()
	}
	r.writer.setString(attrOutputMessages, output)
	if !failed && !soft {
		r.span.SetStatus(codes.Ok, "")
	}
	r.span.End()
}

type traceRunRegistry struct {
	runs sync.Map
}

func (r *traceRunRegistry) add(run *traceRun)    { r.runs.Store(run, struct{}{}) }
func (r *traceRunRegistry) delete(run *traceRun) { r.runs.Delete(run) }

func (r *traceRunRegistry) endAll() {
	r.runs.Range(func(key, _ any) bool {
		key.(*traceRun).end()
		return true
	})
}

// contextIDGenerator honours a trace ID requested through WithTraceID.
type contextIDGenerator struct{}

func (contextIDGenerator) NewIDs(ctx context.Context) (trace.TraceID, trace.SpanID) {
	traceID, _ := ctx.Value(desiredTraceIDKey{}).(trace.TraceID)
	if !traceID.IsValid() {
		traceID = newTraceID()
	}
	return traceID, newSpanID()
}

func (contextIDGenerator) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	return newSpanID()
}

func requestedTraceID(ctx context.Context, configured string) trace.TraceID {
	if configured != "" {
		if traceID, err := trace.TraceIDFromHex(configured); err == nil {
			return traceID
		}
	}
	return trace.SpanContextFromContext(ctx).TraceID()
}

func newTraceID() trace.TraceID {
	for {
		var id trace.TraceID
		_, _ = rand.Read(id[:])
		if id.IsValid() {
			return id
		}
	}
}

func newSpanID() trace.SpanID {
	for {
		var id trace.SpanID
		_, _ = rand.Read(id[:])
		if id.IsValid() {
			return id
		}
	}
}

var _ sdktrace.IDGenerator = contextIDGenerator{}
