// This file contains code adapted from github.com/cloudwego/eino-ext
// (callbacks/langfuse/v2), Copyright 2026 CloudWeGo Authors, licensed under
// the Apache License, Version 2.0. It was modified to emit Tencent Cloud CLS
// Agent Trace spans over OTLP/HTTP instead of Langfuse observations.

package agentobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var (
	_ callbacks.Handler       = (*Handler)(nil)
	_ callbacks.TimingChecker = (*Handler)(nil)
)

type (
	frameKey        struct{}
	currentStateKey struct{}
	// parentToolCallKey carries the tool call ID of the nearest tool span so
	// an agent invoked by that tool can record it as its parent tool call.
	parentToolCallKey struct{}
)

// frame is pushed by every OnStart, including skipped components, so the
// matching end callback can tell its own span apart from an ancestor's.
type frame struct {
	state *spanState
}

type spanState struct {
	start     time.Time
	span      trace.Span
	ctx       context.Context
	owner     *agentScope
	inputDone chan struct{}
	attrs     *spanAttributeWriter
	run       *traceRun
	turn      *turn
	// enclosingAgent owns recovery of failures from this span. Only the
	// outermost agent (or a span outside any agent) can fail the entry span.
	enclosingAgent *agentScope
	scope          *agentScope
	h              *Handler
	step           *stepSpan
	info           *callbacks.RunInfo
	kind           string
	requestModel   string
	// retryEvents is owned by the agent event collector.
	retryEvents []agentRetryEvent
	endOnce     sync.Once
	outcomeMu   sync.Mutex
	// usage is the token usage of a chat span, reported to enclosing agents.
	usage          tokenTotals
	outermostAgent bool
	agentic        bool
	failed         bool
	soft           bool
	// responded is set once a chat span recorded a model response.
	responded bool
}

type agentRetryEvent struct {
	Operation    string `json:"operation"`
	RejectReason any    `json:"reject_reason,omitempty"`
	Error        string `json:"error"`
	Attempt      int    `json:"attempt"`
}

// Needed implements callbacks.TimingChecker. Components that never produce a
// CLS span are filtered here (decided from RunInfo only, so start and end
// callbacks always agree); context-dependent decisions are made in OnStart.
func (h *Handler) Needed(_ context.Context, info *callbacks.RunInfo, _ callbacks.CallbackTiming) bool {
	if info == nil || h.includeFramework {
		return true
	}
	switch info.Component {
	case adk.ComponentOfAgent, adk.ComponentOfAgenticAgent,
		components.ComponentOfChatModel, components.ComponentOfAgenticModel,
		components.ComponentOfTool, components.ComponentOfRetriever, components.ComponentOfEmbedding:
		return true
	case compose.ComponentOfGraph, compose.ComponentOfChain, compose.ComponentOfWorkflow:
		return h.graphAsAgent
	default:
		return false
	}
}

func (h *Handler) OnStart(ctx context.Context, info *callbacks.RunInfo, input callbacks.CallbackInput) (result context.Context) {
	result = ctx
	if info == nil {
		return ctx
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = recoveredPanic("start callback", recovered)
		}
	}()
	ctx, state := h.startSpan(ctx, info)
	result = ctx
	if state == nil {
		return ctx
	}
	defer close(state.inputDone)
	h.setInput(state, []callbacks.CallbackInput{input})
	return ctx
}

func (h *Handler) OnEnd(ctx context.Context, info *callbacks.RunInfo, output callbacks.CallbackOutput) context.Context {
	state := stateFromFrame(ctx)
	if info == nil || state == nil {
		return ctx
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			state.recordCallbackPanic(recoveredPanic("end callback", recovered))
			state.end("")
		}
	}()
	if h.endAgentOutput(state, info, output) {
		return ctx
	}
	state.end(h.setOutput(state, []callbacks.CallbackOutput{output}, time.Time{}))
	return ctx
}

func (h *Handler) OnError(ctx context.Context, info *callbacks.RunInfo, callbackErr error) context.Context {
	state := stateFromFrame(ctx)
	if info == nil || state == nil {
		return ctx
	}
	if callbackErr == nil {
		callbackErr = errors.New("eino callback failed without an error")
	}
	finish := func() {
		defer state.recoverCallbackPanic("error callback", true)
		<-state.inputDone
		state.recordError(callbackErr)
		state.end("")
	}
	if !h.async.run(finish) {
		state.recordError(callbackErr)
		state.end("")
	}
	return ctx
}

func (h *Handler) OnStartWithStreamInput(ctx context.Context, info *callbacks.RunInfo, input *schema.StreamReader[callbacks.CallbackInput]) (result context.Context) {
	result = ctx
	if info == nil {
		closeStream(input)
		return ctx
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			_ = recoveredPanic("stream start callback", recovered)
		}
	}()
	ctx, state := h.startSpan(ctx, info)
	result = ctx
	if state == nil {
		closeStream(input)
		return ctx
	}
	collect := func() {
		defer close(state.inputDone)
		defer state.recoverCallbackPanic("stream input callback", false)
		defer closeStream(input)
		inputs, err := receiveAll(input)
		if err != nil {
			logTelemetryError("stream input", err)
		}
		h.setInput(state, inputs)
	}
	if !h.async.run(collect) {
		closeStream(input)
		close(state.inputDone)
	}
	return ctx
}

func (h *Handler) OnEndWithStreamOutput(ctx context.Context, info *callbacks.RunInfo, output *schema.StreamReader[callbacks.CallbackOutput]) context.Context {
	state := stateFromFrame(ctx)
	if info == nil || state == nil {
		closeStream(output)
		return ctx
	}
	collect := func() {
		defer state.recoverCallbackPanic("stream output callback", true)
		defer closeStream(output)
		outputs, first, err := receiveAllWithFirst(output)
		if err != nil {
			state.recordError(err)
		}
		<-state.inputDone
		state.end(h.setOutput(state, outputs, first))
	}
	if !h.async.run(collect) {
		closeStream(output)
		state.end("")
	}
	return ctx
}

func stateFromFrame(ctx context.Context) *spanState {
	current, _ := ctx.Value(frameKey{}).(*frame)
	if current == nil {
		return nil
	}
	return current.state
}

// classify maps an Eino component to a CLS span kind. An empty kind means the
// component is skipped and its children attach to the nearest kept parent.
func (h *Handler) classify(ctx context.Context, info *callbacks.RunInfo) (kind, source string) {
	switch info.Component {
	case adk.ComponentOfAgent, adk.ComponentOfAgenticAgent:
		return SpanKindAgent, agentSourceADK
	case components.ComponentOfChatModel, components.ComponentOfAgenticModel:
		return SpanKindChat, ""
	case components.ComponentOfTool:
		return SpanKindTool, ""
	case components.ComponentOfRetriever:
		return SpanKindRetriever, ""
	case components.ComponentOfEmbedding:
		return SpanKindEmbedding, ""
	case compose.ComponentOfGraph, compose.ComponentOfChain, compose.ComponentOfWorkflow:
		if h.graphAsAgent && ctx.Value(currentStateKey{}) == nil {
			return SpanKindAgent, agentSourceGraph
		}
	}
	if h.includeFramework {
		return SpanKindTask, ""
	}
	return "", ""
}

func (h *Handler) startSpan(ctx context.Context, info *callbacks.RunInfo) (context.Context, *spanState) {
	kind, source := h.classify(ctx, info)
	if kind == "" {
		return context.WithValue(ctx, frameKey{}, &frame{}), nil
	}

	ctxRun, _ := ctx.Value(traceRunKey{}).(*traceRun)
	run := ctxRun
	if run != nil && !run.childStarted() {
		run = nil
	}
	tn, _ := ctx.Value(turnKey{}).(*turn)
	enclosing, _ := ctx.Value(agentScopeKey{}).(*agentScope)
	if enclosing != nil && enclosing.run != ctxRun {
		// StartTrace may introduce a new entry inside an existing agent context.
		enclosing = nil
	}
	owner, _ := ctx.Value(stepOwnerKey{}).(*agentScope)
	if owner != nil && owner.run != ctxRun {
		owner = nil
	}

	var step *stepSpan
	switch kind {
	case SpanKindChat:
		if owner != nil {
			step = owner.beginStep()
		}
	case SpanKindTool:
		if owner != nil {
			step = owner.stepForToolCall(compose.GetToolCallID(ctx))
			owner.toolCalls.Add(1)
		}
	}
	step.childStarted()

	start := time.Now()
	spanCtx, span := h.tracer.Start(step.parentContext(ctx), spanName(kind, info, ""),
		trace.WithSpanKind(otelSpanKind(kind)), trace.WithTimestamp(start))
	if tn == nil {
		defaults := h.defaults
		tn = newTurn(&defaults, span.SpanContext().TraceID())
		spanCtx = context.WithValue(spanCtx, turnKey{}, tn)
	}

	w := newSpanAttributeWriter(span, h.prepare, h.maxSpanAttributeBytes, h.losses)
	w.setString(attrSpanKind, kind)
	operation := operationFor(kind)
	if kind == SpanKindAgent && enclosing != nil {
		operation = OperationInvokeSubagent
	}
	w.setString(attrOperationName, operation)
	tn.apply(w)
	w.setString(attrEinoComponent, string(info.Component))
	w.setString(attrEinoType, info.Type)

	state := &spanState{
		h:              h,
		kind:           kind,
		info:           info,
		agentic:        info.Component == components.ComponentOfAgenticModel,
		span:           span,
		attrs:          w,
		run:            run,
		turn:           tn,
		enclosingAgent: enclosing,
		step:           step,
		start:          start,
		inputDone:      make(chan struct{}),
	}

	switch kind {
	case SpanKindAgent:
		scope := &agentScope{
			h:      h,
			state:  state,
			parent: enclosing,
			id:     span.SpanContext().SpanID().String(),
			name:   agentName(info),
			turn:   tn,
			run:    ctxRun,
			source: source,
		}
		state.scope = scope
		state.outermostAgent = enclosing == nil
		w.setString(attrAgentID, scope.id)
		w.setString(attrAgentName, scope.name)
		w.setString(attrEinoAgentSource, source)
		if enclosing != nil {
			w.setString(attrAgentScope, agentScopeSubagent)
			if toolCallID, _ := ctx.Value(parentToolCallKey{}).(string); toolCallID != "" {
				w.setString(attrSubagentParentToolCallID, toolCallID)
			}
		}
		spanCtx = context.WithValue(spanCtx, agentScopeKey{}, scope)
		spanCtx = context.WithValue(spanCtx, stepOwnerKey{}, scope)
		// Agents nested directly in this one were not invoked by a tool.
		spanCtx = context.WithValue(spanCtx, parentToolCallKey{}, "")
	case SpanKindTool:
		toolCallID := compose.GetToolCallID(ctx)
		if toolCallID == "" {
			toolCallID = span.SpanContext().SpanID().String()
		}
		w.setString(attrToolName, info.Name)
		w.setString(attrToolType, toolTypeFunction)
		w.setString(attrToolCallID, toolCallID)
		// Model calls nested inside a tool are not ReAct rounds of the
		// enclosing agent; a nested agent creates its own scope.
		spanCtx = context.WithValue(spanCtx, stepOwnerKey{}, (*agentScope)(nil))
		spanCtx = context.WithValue(spanCtx, parentToolCallKey{}, toolCallID)
	case SpanKindChat:
		state.owner = owner
		w.setString(attrProviderName, strings.ToLower(info.Type))
		for scope := enclosing; scope != nil; scope = scope.parent {
			scope.chatStarted()
		}
	}
	if kind != SpanKindAgent && enclosing != nil {
		w.setString(attrAgentID, enclosing.id)
		w.setString(attrAgentName, enclosing.name)
	}
	step.applyTo(w)

	spanCtx = context.WithValue(spanCtx, currentStateKey{}, state)
	spanCtx = context.WithValue(spanCtx, frameKey{}, &frame{state: state})
	state.ctx = spanCtx
	return spanCtx, state
}

func (h *Handler) setInput(state *spanState, inputs []callbacks.CallbackInput) {
	w := state.attrs
	switch state.kind {
	case SpanKindChat:
		h.setChatInput(state, inputs)
	case SpanKindTool:
		if h.captureContent {
			w.setContent(attrToolCallArguments, toolArguments(inputs))
		}
	case SpanKindAgent:
		h.setAgentInput(state, inputs)
	case SpanKindRetriever:
		if len(inputs) == 0 {
			return
		}
		if converted := retriever.ConvCallbackInput(inputs[0]); converted != nil {
			if h.captureContent {
				w.setContent(attrRetrievalQueryText, converted.Query)
			}
			if converted.TopK > 0 {
				w.setInt(attrRetrievalTopK, converted.TopK)
			}
		}
	case SpanKindEmbedding:
		if len(inputs) == 0 {
			return
		}
		if converted := embedding.ConvCallbackInput(inputs[0]); converted != nil {
			w.setInt(attrEmbeddingsInputCount, len(converted.Texts))
			if converted.Config != nil && converted.Config.Model != "" {
				// Model fields are written on output only, so a failed call
				// shows no model; the span name keeps it for troubleshooting.
				state.requestModel = converted.Config.Model
				state.span.SetName(OperationEmbeddings + " " + converted.Config.Model)
			}
		}
	default:
		if h.captureContent {
			h.setGenericJSON(w, attrEinoInput, collapseValues(inputs))
		}
	}
}

func (h *Handler) setChatInput(state *spanState, inputs []callbacks.CallbackInput) {
	w := state.attrs
	request, err := extractChatRequest(state.agentic, inputs)
	if err != nil {
		logTelemetryError("model input", err)
	}
	if request == nil {
		return
	}
	if request.model != "" {
		// gen_ai.request.model is written with the response (see
		// setChatOutput) so a failed call shows no model.
		state.requestModel = request.model
		state.span.SetName(spanName(SpanKindChat, state.info, request.model))
	}
	if request.temperature != nil {
		w.setFloat(attrRequestTemperature, float64(*request.temperature))
	}
	if request.maxTokens > 0 {
		w.setInt(attrRequestMaxTokens, request.maxTokens)
	}
	if request.topP != nil {
		w.setFloat(attrRequestTopP, float64(*request.topP))
	}
	w.setStringSlice(attrRequestStopSequences, request.stop)
	if len(request.tools) > 0 {
		w.setJSONContent(attrToolDefinitions, convertToolDefinitions(request.tools))
	}
	if h.captureContent && len(request.messages) > 0 {
		serialized := w.setJSONContent(attrInputMessages, request.messages)
		w.setString(attrInputMessagesHash, contentHash(serialized))
		if state.run != nil {
			state.run.offerInput(h.prepareJSON(lastUserMessage(request.messages)))
		}
	}
}

func (h *Handler) setAgentInput(state *spanState, inputs []callbacks.CallbackInput) {
	if len(inputs) == 0 {
		return
	}
	w := state.attrs
	var messages []genAIMessage
	switch state.info.Component {
	case adk.ComponentOfAgent:
		converted := adk.ConvAgentCallbackInput(inputs[0])
		if converted == nil {
			return
		}
		if converted.ResumeInfo != nil {
			w.setBool(attrEinoAgentResumed, true)
		}
		if converted.Input != nil {
			messages = convertMessages(converted.Input.Messages)
		}
	case adk.ComponentOfAgenticAgent:
		converted := adk.ConvTypedCallbackInput[*schema.AgenticMessage](inputs[0])
		if converted == nil {
			return
		}
		if converted.ResumeInfo != nil {
			w.setBool(attrEinoAgentResumed, true)
		}
		if converted.Input != nil {
			messages = convertAgenticMessages(converted.Input.Messages)
		}
	default:
		var ok bool
		messages, ok = messagesFromValues(inputs, "user")
		if !ok && h.captureContent {
			h.setGenericJSON(w, attrEinoInput, collapseValues(inputs))
		}
	}
	if !h.captureContent || len(messages) == 0 {
		return
	}
	serialized := w.setJSONContent(attrInputMessages, messages)
	w.setString(attrInputMessagesHash, contentHash(serialized))
	if state.run != nil {
		state.run.offerInput(h.prepareJSON(lastUserMessage(messages)))
	}
}

// setOutput records the component output and returns the serialized value
// offered to the entry span as its output candidate.
func (h *Handler) setOutput(state *spanState, outputs []callbacks.CallbackOutput, firstChunk time.Time) string {
	w := state.attrs
	switch state.kind {
	case SpanKindChat:
		return h.setChatOutput(state, outputs, firstChunk)
	case SpanKindTool:
		if h.captureContent {
			w.setContent(attrToolCallResult, toolResult(outputs))
		}
	case SpanKindAgent:
		messages, ok := messagesFromValues(outputs, "assistant")
		if !ok {
			if h.captureContent {
				h.setGenericJSON(w, attrEinoOutput, collapseValues(outputs))
			}
			return ""
		}
		state.scope.messageCount.Store(int64(len(messages)))
		if !h.captureContent {
			return ""
		}
		w.setJSONContent(attrOutputMessages, messages)
		return h.prepareJSON(finalAssistantMessage(messages))
	case SpanKindRetriever:
		h.setRetrieverOutput(state, outputs)
	case SpanKindEmbedding:
		h.setEmbeddingOutput(state, outputs)
	default:
		if h.captureContent {
			h.setGenericJSON(w, attrEinoOutput, collapseValues(outputs))
		}
	}
	return ""
}

func (h *Handler) setChatOutput(state *spanState, outputs []callbacks.CallbackOutput, firstChunk time.Time) string {
	w := state.attrs
	response, err := extractChatResponse(state.agentic, outputs)
	if err != nil {
		logTelemetryError("model output", err)
	}
	if response == nil {
		return ""
	}
	// A stream that failed or was cancelled mid-way records its error before
	// the partial response arrives here.
	failed, soft := state.outcome()
	responseModel := firstNonEmpty(response.model, state.requestModel)
	if responseModel != "" {
		w.setString(attrResponseModel, responseModel)
		w.setString(attrRequestModel, firstNonEmpty(state.requestModel, responseModel))
		if state.requestModel == "" {
			state.span.SetName(spanName(SpanKindChat, state.info, responseModel))
		}
	}
	reasons := finishReasons(response.messages)
	if len(reasons) == 0 {
		reasons = terminalFinishReasons(failed, soft)
	}
	w.setStringSlice(attrFinishReasons, reasons)
	usage := response.usage
	if usage == nil && (failed || soft) {
		usage = &schema.TokenUsage{}
	}
	totals := setUsage(w, usage)
	state.outcomeMu.Lock()
	state.responded, state.usage = true, totals
	state.outcomeMu.Unlock()
	if firstChunk.IsZero() && !failed {
		// A non-streaming call delivers its first token with the response.
		firstChunk = time.Now()
	}
	if !firstChunk.IsZero() {
		w.setInt64(attrTimeToFirstTokenMs, firstChunk.Sub(state.start).Milliseconds())
	}
	if len(response.messages) > 0 {
		last := response.messages[len(response.messages)-1]
		if state.step != nil {
			reason := reactFinishReason(last)
			if failed {
				reason = finishReasonError
			}
			w.setString(attrReactFinishReason, reason)
			state.step.setFinishReason(reason)
			state.owner.bindToolCalls(state.step, toolCallIDs(last))
		}
	}
	if !h.captureContent || len(response.messages) == 0 {
		return ""
	}
	return w.setJSONContent(attrOutputMessages, response.messages)
}

type retrievedDocument struct {
	ID      string  `json:"id,omitempty"`
	Content string  `json:"content,omitempty"`
	Score   float64 `json:"score"`
}

func (h *Handler) setRetrieverOutput(state *spanState, outputs []callbacks.CallbackOutput) {
	if len(outputs) == 0 {
		return
	}
	converted := retriever.ConvCallbackOutput(outputs[0])
	if converted == nil {
		return
	}
	documents := make([]retrievedDocument, 0, len(converted.Docs))
	for _, doc := range converted.Docs {
		if doc != nil {
			documents = append(documents, retrievedDocument{ID: doc.ID, Score: doc.Score(), Content: doc.Content})
		}
	}
	// The count carries no content, so it is kept with capture disabled.
	state.attrs.setInt(attrRetrievalDocumentsCount, len(documents))
	if h.captureContent {
		state.attrs.setJSONContent(attrRetrievalDocuments, documents)
	}
}

func (h *Handler) setEmbeddingOutput(state *spanState, outputs []callbacks.CallbackOutput) {
	if len(outputs) == 0 {
		return
	}
	converted := embedding.ConvCallbackOutput(outputs[0])
	if converted == nil {
		return
	}
	w := state.attrs
	if len(converted.Embeddings) > 0 && len(converted.Embeddings[0]) > 0 {
		w.setInt(attrEmbeddingsDimensionCount, len(converted.Embeddings[0]))
	}
	if state.requestModel != "" {
		w.setString(attrRequestModel, state.requestModel)
	}
	if converted.Config != nil && converted.Config.Model != "" {
		w.setString(attrResponseModel, converted.Config.Model)
	}
	if usage := converted.TokenUsage; usage != nil {
		w.setInt(attrUsageInputTokens, usage.PromptTokens)
		w.setInt(attrUsageTotalTokens, usage.TotalTokens)
	}
}

func (h *Handler) endAgentOutput(state *spanState, info *callbacks.RunInfo, output callbacks.CallbackOutput) bool {
	if state.kind != SpanKindAgent {
		return false
	}
	var collect func() string
	switch info.Component {
	case adk.ComponentOfAgent:
		converted := adk.ConvAgentCallbackOutput(output)
		if converted == nil || converted.Events == nil {
			return false
		}
		collect = func() string { return h.collectAgentMessages(state, converted.Events) }
	case adk.ComponentOfAgenticAgent:
		converted := adk.ConvTypedCallbackOutput[*schema.AgenticMessage](output)
		if converted == nil || converted.Events == nil {
			return false
		}
		collect = func() string { return h.collectAgenticMessages(state, converted.Events) }
	default:
		return false
	}
	// The event iterator must be drained asynchronously so the agent is not
	// blocked; the agent span ends when the iterator is exhausted.
	if !h.async.run(func() {
		defer state.recoverCallbackPanic("agent output callback", true)
		<-state.inputDone
		state.end(collect())
	}) {
		state.end("")
	}
	return true
}

func (h *Handler) collectAgentMessages(state *spanState, events *adk.AsyncIterator[*adk.AgentEvent]) string {
	var messages []*schema.Message
	var customized []any
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event == nil {
			continue
		}
		if event.Err != nil {
			state.recordAgentError("agent output", event.Err)
		}
		if event.Output == nil {
			continue
		}
		if event.Output.MessageOutput != nil {
			message, err := variantMessage(event.Output.MessageOutput, concatMessageChunks)
			if err != nil {
				state.recordAgentError("agent message", err)
			} else if message != nil {
				messages = append(messages, message)
			}
		}
		if event.Output.CustomizedOutput != nil {
			customized = append(customized, event.Output.CustomizedOutput)
		}
	}
	return h.setAgentOutput(state, convertMessages(messages), customized)
}

func (h *Handler) collectAgenticMessages(state *spanState, events *adk.AsyncIterator[*adk.TypedAgentEvent[*schema.AgenticMessage]]) string {
	var messages []*schema.AgenticMessage
	var customized []any
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event == nil {
			continue
		}
		if event.Err != nil {
			state.recordAgentError("agent output", event.Err)
		}
		if event.Output == nil {
			continue
		}
		if event.Output.MessageOutput != nil {
			message, err := variantMessage(event.Output.MessageOutput, concatAgenticChunks)
			if err != nil {
				state.recordAgentError("agent message", err)
			} else if message != nil {
				messages = append(messages, message)
			}
		}
		if event.Output.CustomizedOutput != nil {
			customized = append(customized, event.Output.CustomizedOutput)
		}
	}
	return h.setAgentOutput(state, convertAgenticMessages(messages), customized)
}

// variantMessage is MessageVariant.GetMessage without reading the shared
// chunks' Extra field (see detachMessage). The callback receives its own copy
// of the stream, so draining it does not affect the caller.
func variantMessage[M adk.MessageType](variant *adk.TypedMessageVariant[M], concat func([]M) (M, error)) (M, error) {
	if !variant.IsStreaming {
		return variant.Message, nil
	}
	var zero M
	if variant.MessageStream == nil {
		return zero, nil
	}
	defer variant.MessageStream.Close()
	chunks, err := receiveAll(variant.MessageStream)
	if err != nil {
		return zero, err
	}
	return concat(chunks)
}

func (h *Handler) setAgentOutput(state *spanState, messages []genAIMessage, customized []any) string {
	w := state.attrs
	state.scope.messageCount.Store(int64(len(messages)))
	var final string
	if h.captureContent {
		if len(messages) > 0 {
			w.setJSONContent(attrOutputMessages, messages)
			final = h.prepareJSON(finalAssistantMessage(messages))
		}
		if values := nonEmptySlice(customized); values != nil {
			h.setGenericJSON(w, attrEinoAgentCustomized, values)
		}
	}
	// Keep the output before spending remaining budget on diagnostics. The
	// count is of retry notifications, not executed retries.
	if len(state.retryEvents) > 0 {
		w.setInt(attrEinoRetryEventCount, len(state.retryEvents))
		h.setGenericJSON(w, attrEinoRetryEvents, state.retryEvents)
	}
	return final
}

func (h *Handler) setGenericJSON(w *spanAttributeWriter, key string, value any) {
	encoded, err := json.Marshal(value)
	if err != nil {
		h.losses.Add(lossMetadataSerialization, 1)
		return
	}
	w.setContent(key, string(encoded))
}

func (h *Handler) prepareJSON(value any) string {
	if value == nil || reflect.ValueOf(value).Kind() == reflect.Slice && reflect.ValueOf(value).Len() == 0 {
		return ""
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		h.losses.Add(lossPayloadSerialization, 1)
		return ""
	}
	return h.prepare(string(encoded))
}

func (s *spanState) end(output string) {
	s.endOnce.Do(func() {
		// An agent span waits for model calls in its subtree so its token
		// totals are complete; the last of them finishes the agent.
		if s.scope != nil && s.scope.deferEnd(func() { s.finish(output) }) {
			return
		}
		s.finish(output)
	})
}

func (s *spanState) finish(output string) {
	// Deferred calls run in reverse: the step closes first, then enclosing
	// agents learn the chat usage (possibly ending them), then the entry.
	if s.run != nil {
		defer s.run.childEnded(output, s.outermostAgent)
	}
	if s.kind == SpanKindChat {
		defer s.reportUsage()
	}
	defer s.step.childEnded()
	if s.scope != nil {
		s.scope.finish()
	}
	elapsed := time.Since(s.start).Milliseconds()
	s.outcomeMu.Lock()
	failed, soft, responded := s.failed, s.soft, s.responded
	s.outcomeMu.Unlock()
	switch s.kind {
	case SpanKindChat:
		s.attrs.setInt64(attrChatDurationMs, elapsed)
		if !responded && (failed || soft) {
			s.attrs.setStringSlice(attrFinishReasons, terminalFinishReasons(failed, soft))
			setUsage(s.attrs, &schema.TokenUsage{})
			if failed && s.step != nil {
				s.attrs.setString(attrReactFinishReason, finishReasonError)
			}
		}
	case SpanKindTool:
		s.attrs.setInt64(attrToolCallDurationMs, elapsed)
	}
	if !failed && !soft {
		s.span.SetStatus(codes.Ok, "")
	}
	s.span.End()
}

func (s *spanState) reportUsage() {
	s.outcomeMu.Lock()
	usage := s.usage
	s.outcomeMu.Unlock()
	for scope := s.enclosingAgent; scope != nil; scope = scope.parent {
		scope.chatEnded(usage)
	}
}

func (s *spanState) outcome() (failed, soft bool) {
	s.outcomeMu.Lock()
	defer s.outcomeMu.Unlock()
	return s.failed, s.soft
}

// terminalFinishReasons describes a model call that ended without a provider
// finish reason because it failed or was cancelled.
func terminalFinishReasons(failed, soft bool) []string {
	switch {
	case failed:
		return []string{finishReasonError}
	case soft:
		return []string{finishReasonCancelled}
	default:
		return nil
	}
}

// recoverCallbackPanic runs inside collector goroutines. Terminal collectors
// always end the span so the entry span is never left waiting.
func (s *spanState) recoverCallbackPanic(operation string, terminal bool) {
	if recovered := recover(); recovered != nil {
		if terminal {
			<-s.inputDone
			defer s.end("")
		}
		s.recordCallbackPanic(recoveredPanic(operation, recovered))
	}
}

// recordCallbackPanic records a telemetry-internal failure. It does not mark
// the business operation as failed.
func (s *spanState) recordCallbackPanic(err error) {
	if s.run != nil {
		s.run.recordCallbackPanic(err.Error())
	}
	s.attrs.setString(attrEinoCallbackPanic, err.Error())
}

func (s *spanState) recordAgentError(operation string, err error) {
	var retryErr *adk.WillRetryError
	if errors.As(err, &retryErr) {
		reason := retryErr.RejectReason()
		if cause, ok := reason.(error); ok {
			reason = cause.Error()
		}
		if reason != nil {
			if encoded, marshalErr := json.Marshal(reason); marshalErr != nil {
				s.h.losses.Add(lossMetadataSerialization, 1)
				reason = fmt.Sprint(reason)
			} else {
				reason = json.RawMessage(encoded)
			}
		}
		s.retryEvents = append(s.retryEvents, agentRetryEvent{
			Operation:    operation,
			Attempt:      retryErr.RetryAttempt,
			Error:        retryErr.Error(),
			RejectReason: reason,
		})
		return
	}
	s.recordError(err)
}

// recordError classifies err as cancellation, resumable interrupt or failure.
func (s *spanState) recordError(err error) {
	if err == nil {
		return
	}
	if cause, cancelled := cancellationCause(s.ctx, err); cancelled {
		s.markSoft(statusCancelled, cause.Error())
		return
	}
	if causeText, interrupted := interruptionCause(err); interrupted {
		s.markSoft(statusInterrupted, causeText)
		return
	}
	s.outcomeMu.Lock()
	alreadyFailed := s.failed
	s.failed = true
	s.outcomeMu.Unlock()
	if alreadyFailed {
		return
	}
	message := s.h.prepare(err.Error())
	kindOfError := errorType(s.kind, err)
	s.span.RecordError(err)
	s.span.SetStatus(codes.Error, message)
	s.attrs.setString(attrErrorType, kindOfError)
	s.attrs.setString(attrErrorMessage, message)
	switch s.kind {
	case SpanKindTool:
		s.attrs.setString(attrToolErrorType, kindOfError)
	case SpanKindChat:
		s.step.recordModelError(message)
	}
	if s.run != nil && s.enclosingAgent == nil {
		s.run.recordError(message, err)
	}
}

func (s *spanState) markSoft(status, cause string) {
	s.outcomeMu.Lock()
	s.soft = true
	s.outcomeMu.Unlock()
	s.attrs.setString(attrEinoStatus, status)
	s.attrs.setContent(attrEinoStatusCause, cause)
	if s.run != nil {
		s.run.recordSoftStatus(status, cause)
	}
}

func errorType(kind string, err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	switch kind {
	case SpanKindTool:
		return "tool_error"
	case SpanKindChat:
		return "chat_error"
	case SpanKindAgent:
		return "agent_error"
	case SpanKindRetriever:
		return "retrieval_error"
	case SpanKindEmbedding:
		return "embedding_error"
	default:
		return "error"
	}
}

func spanName(kind string, info *callbacks.RunInfo, model string) string {
	switch kind {
	case SpanKindAgent:
		return OperationInvokeAgent + " " + agentName(info)
	case SpanKindChat:
		if model != "" {
			return OperationChat + " " + model
		}
		return OperationChat
	case SpanKindTool:
		return OperationExecuteTool + " " + defaultString(info.Name, "unknown")
	case SpanKindRetriever:
		return OperationRetrieval + " " + callbackName(info)
	case SpanKindEmbedding:
		return OperationEmbeddings + " " + callbackName(info)
	default:
		return callbackName(info)
	}
}

func agentName(info *callbacks.RunInfo) string {
	return firstNonEmpty(info.Name, info.Type, "agent")
}

func callbackName(info *callbacks.RunInfo) string {
	switch {
	case info.Name != "":
		return info.Name
	case info.Type != "":
		return info.Type + string(info.Component)
	case info.Component != "":
		return string(info.Component)
	default:
		return "eino-operation"
	}
}

func otelSpanKind(kind string) trace.SpanKind {
	switch kind {
	case SpanKindChat, SpanKindTool, SpanKindRetriever, SpanKindEmbedding:
		return trace.SpanKindClient
	default:
		return trace.SpanKindInternal
	}
}

func toolArguments(inputs []callbacks.CallbackInput) string {
	var builder strings.Builder
	for _, input := range inputs {
		if converted := tool.ConvCallbackInput(input); converted != nil {
			builder.WriteString(converted.ArgumentsInJSON)
		}
	}
	return builder.String()
}

func toolResult(outputs []callbacks.CallbackOutput) string {
	var builder strings.Builder
	var results []*schema.ToolResult
	for _, output := range outputs {
		converted := tool.ConvCallbackOutput(output)
		if converted == nil {
			continue
		}
		builder.WriteString(converted.Response)
		if converted.ToolOutput != nil {
			results = append(results, converted.ToolOutput)
		}
	}
	if builder.Len() > 0 || len(results) == 0 {
		return builder.String()
	}
	result := results[0]
	if len(results) > 1 {
		if concatenated, err := schema.ConcatToolResults(results); err == nil {
			result = concatenated
		}
	}
	parts := make([]genAIPart, 0, len(result.Parts))
	for _, part := range result.Parts {
		switch {
		case part.Type == schema.ToolPartTypeText:
			parts = append(parts, genAIPart{Type: "text", Content: part.Text})
		default:
			parts = append(parts, genAIPart{Type: "blob", Modality: string(part.Type)})
		}
	}
	encoded, err := json.Marshal(parts)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// messagesFromValues converts graph inputs/outputs that carry messages (or a
// plain string) into GenAI messages. Streamed message chunks are concatenated.
func messagesFromValues[T any](values []T, textRole string) ([]genAIMessage, bool) {
	if len(values) == 1 {
		switch value := any(values[0]).(type) {
		case []*schema.Message:
			return convertMessages(value), true
		case []*schema.AgenticMessage:
			return convertAgenticMessages(value), true
		case string:
			if value == "" {
				return nil, false
			}
			return []genAIMessage{textMessage(textRole, value)}, true
		}
	}
	var chunks []*schema.Message
	for _, value := range values {
		message, ok := any(value).(*schema.Message)
		if !ok {
			return nil, false
		}
		if message != nil {
			chunks = append(chunks, message)
		}
	}
	switch len(chunks) {
	case 0:
		return nil, false
	case 1:
		return convertMessages(chunks), true
	}
	message, err := concatMessageChunks(chunks)
	if err != nil {
		return nil, false
	}
	return convertMessages([]*schema.Message{message}), true
}

func interruptionCause(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	if _, interrupted := compose.IsInterruptRerunError(err); interrupted {
		return err.Error(), true
	}
	if _, interrupted := compose.ExtractInterruptInfo(err); interrupted {
		return err.Error(), true
	}
	var adkInterrupt *adk.InterruptError
	if errors.As(err, &adkInterrupt) {
		return err.Error(), true
	}
	return "", false
}

func cancellationCause(ctx context.Context, err error) (error, bool) {
	if errors.Is(err, context.Canceled) {
		if ctx != nil {
			if cause := context.Cause(ctx); cause != nil {
				return cause, true
			}
		}
		return context.Canceled, true
	}
	if ctx == nil || !errors.Is(ctx.Err(), context.Canceled) {
		return nil, false
	}
	cause := context.Cause(ctx)
	if cause != nil && errors.Is(err, cause) {
		return cause, true
	}
	return nil, false
}

func collapseValues[T any](values []T) any {
	if len(values) == 1 {
		return values[0]
	}
	return values
}

func nonEmptySlice(value any) any {
	reflected := reflect.ValueOf(value)
	if reflected.Kind() != reflect.Slice || reflected.Len() == 0 {
		return nil
	}
	if reflected.Len() == 1 {
		return reflected.Index(0).Interface()
	}
	return value
}

func receiveAll[T any](reader *schema.StreamReader[T]) ([]T, error) {
	values, _, err := receiveAllWithFirst(reader)
	return values, err
}

func receiveAllWithFirst[T any](reader *schema.StreamReader[T]) ([]T, time.Time, error) {
	if reader == nil {
		return nil, time.Time{}, errors.New("stream is nil")
	}
	var values []T
	var first time.Time
	for {
		value, err := reader.Recv()
		if errors.Is(err, io.EOF) {
			return values, first, nil
		}
		if err != nil {
			return values, first, err
		}
		if first.IsZero() {
			first = time.Now()
		}
		values = append(values, value)
	}
}

func closeStream[T any](reader *schema.StreamReader[T]) {
	if reader != nil {
		reader.Close()
	}
}

func logTelemetryError(operation string, err error) {
	log.Printf("agentobs: failed to parse %s: %v", operation, err)
}

type asyncGroup struct {
	idle    chan struct{}
	active  int
	mu      sync.Mutex
	closing bool
}

func newAsyncGroup() asyncGroup {
	idle := make(chan struct{})
	close(idle)
	return asyncGroup{idle: idle}
}

func (g *asyncGroup) run(fn func()) bool {
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		return false
	}
	if g.active == 0 {
		g.idle = make(chan struct{})
	}
	g.active++
	g.mu.Unlock()
	go func() {
		defer g.done()
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = recoveredPanic("async callback", recovered)
			}
		}()
		fn()
	}()
	return true
}

func (g *asyncGroup) done() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.active--
	if g.active == 0 {
		close(g.idle)
	}
}

func (g *asyncGroup) wait(ctx context.Context, closeGroup bool) error {
	g.mu.Lock()
	if closeGroup {
		g.closing = true
	}
	idle := g.idle
	g.mu.Unlock()
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
