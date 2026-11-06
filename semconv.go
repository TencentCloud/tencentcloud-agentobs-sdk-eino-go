package agentobs

// CLS gen_ai.span.kind values. Kinds after SpanKindTool are extensions for
// Eino components that the CLS specification does not define yet.
const (
	SpanKindEntry     = "entry"
	SpanKindAgent     = "agent"
	SpanKindStep      = "step"
	SpanKindChat      = "chat"
	SpanKindTool      = "tool"
	SpanKindRetriever = "retriever"
	SpanKindEmbedding = "embedding"
	SpanKindTask      = "task"
)

// CLS gen_ai.operation.name values.
const (
	OperationEnterApplication = "enter_application"
	OperationInvokeAgent      = "invoke_agent"
	OperationInvokeSubagent   = "invoke_subagent"
	OperationReact            = "react"
	OperationChat             = "chat"
	OperationExecuteTool      = "execute_tool"
	OperationRetrieval        = "retrieval"
	OperationEmbeddings       = "embeddings"
	OperationInvoke           = "invoke"
)

const (
	attrSpanKind      = "gen_ai.span.kind"
	attrOperationName = "gen_ai.operation.name"
	attrAgentType     = "gen_ai.agent.type"
	attrAgentID       = "gen_ai.agent.id"
	attrAgentName     = "gen_ai.agent.name"
	attrSessionID     = "gen_ai.session.id"
	attrTurnID        = "gen_ai.turn.id"
	attrStepID        = "gen_ai.step.id"
	attrUserID        = "gen_ai.user.id"
	attrUserName      = "gen_ai.user.name"
	attrEntryType     = "gen_ai.entry.type"

	attrProviderName         = "gen_ai.provider.name"
	attrRequestModel         = "gen_ai.request.model"
	attrRequestTemperature   = "gen_ai.request.temperature"
	attrRequestMaxTokens     = "gen_ai.request.max_tokens"
	attrRequestTopP          = "gen_ai.request.top_p"
	attrRequestStopSequences = "gen_ai.request.stop_sequences"
	attrResponseModel        = "gen_ai.response.model"
	attrFinishReasons        = "gen_ai.response.finish_reasons"
	attrTimeToFirstTokenMs   = "gen_ai.response.time_to_first_token_ms"
	attrChatDurationMs       = "gen_ai.chat.duration_ms"
	attrInputMessages        = "gen_ai.input.messages"
	attrInputMessagesHash    = "gen_ai.input.messages.hash"
	attrOutputMessages       = "gen_ai.output.messages"
	attrToolDefinitions      = "gen_ai.tool.definitions"

	attrUsageInputTokens         = "gen_ai.usage.input_tokens"
	attrUsageOutputTokens        = "gen_ai.usage.output_tokens"
	attrUsageTotalTokens         = "gen_ai.usage.total_tokens"
	attrUsageCacheReadTokens     = "gen_ai.usage.cache_read.input_tokens"
	attrUsageCacheMissTokens     = "gen_ai.usage.cache_miss.input_tokens"
	attrUsageReasoningTokens     = "gen_ai.usage.reasoning_output_tokens"
	attrAgentMessageCount        = "gen_ai.agent.message_count"
	attrAgentToolCallCount       = "gen_ai.agent.tool_call_count"
	attrAgentScope               = "gen_ai.agent.scope"
	attrSubagentParentToolCallID = "gen_ai.subagent.parent_tool_call.id"
	attrToolCallID               = "gen_ai.tool.call.id"
	attrToolName                 = "gen_ai.tool.name"
	attrToolType                 = "gen_ai.tool.type"
	attrToolCallArguments        = "gen_ai.tool.call.arguments"
	attrToolCallResult           = "gen_ai.tool.call.result"
	attrToolCallDurationMs       = "gen_ai.tool.call.duration_ms"
	attrToolErrorType            = "gen_ai.tool.error.type"
	attrReactRound               = "gen_ai.react.round"
	attrReactFinishReason        = "gen_ai.react.finish_reason"
	attrRetrievalQueryText       = "gen_ai.retrieval.query_text"
	attrRetrievalTopK            = "gen_ai.retrieval.top_k"
	attrRetrievalDocuments       = "gen_ai.retrieval.documents"
	attrRetrievalDocumentsCount  = "gen_ai.retrieval.documents.count"
	attrEmbeddingsDimensionCount = "gen_ai.embeddings.dimension.count"
	attrEmbeddingsInputCount     = "gen_ai.embeddings.input.count"
	attrErrorType                = "error.type"
	attrErrorMessage             = "error.message"

	// Eino-specific extensions.
	attrEinoComponent       = "eino.component"
	attrEinoType            = "eino.type"
	attrEinoInput           = "eino.input"
	attrEinoOutput          = "eino.output"
	attrEinoAgentSource     = "eino.agent.source"
	attrEinoAgentResumed    = "eino.agent.resumed"
	attrEinoAgentCustomized = "eino.agent.customized_output"
	attrEinoStatus          = "eino.status"
	attrEinoStatusCause     = "eino.status.cause"
	attrEinoRetryEvents     = "eino.retry.events"
	attrEinoRetryEventCount = "eino.retry.event_count"
	attrEinoCallbackPanic   = "eino.callback.panic"
)

const (
	agentSourceADK   = "adk"
	agentSourceGraph = "graph"

	statusCancelled   = "cancelled"
	statusInterrupted = "interrupted"

	agentScopeSubagent = "subagent"

	// Finish reasons written when a model call ends without a response.
	finishReasonError     = "error"
	finishReasonCancelled = "cancelled"

	toolTypeFunction = "function"
)

func operationFor(kind string) string {
	switch kind {
	case SpanKindEntry:
		return OperationEnterApplication
	case SpanKindAgent:
		return OperationInvokeAgent
	case SpanKindStep:
		return OperationReact
	case SpanKindChat:
		return OperationChat
	case SpanKindTool:
		return OperationExecuteTool
	case SpanKindRetriever:
		return OperationRetrieval
	case SpanKindEmbedding:
		return OperationEmbeddings
	default:
		return OperationInvoke
	}
}
