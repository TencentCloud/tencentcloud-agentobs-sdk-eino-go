# tencentcloud-agentobs-sdk-eino-go

English | [中文](README_zh.md)

Converts [CloudWeGo Eino](https://github.com/cloudwego/eino) callbacks into OpenTelemetry traces that follow the **Tencent Cloud CLS Agent Trace field specification**, and exports them over OTLP/HTTP to CLS (Cloud Log Service) Agent Observability.

- Supports ADK agents (`ChatModelAgent`, agentic agents, nested AgentTool), Graph / Chain / Workflow, ChatModel / AgenticModel, Tool, Retriever and Embedding
- Synthesizes ReAct `step` spans: one step per model call, with tools attached by `tool_call_id` to the round that requested them
- Streaming input/output with time to first token (TTFT); streams are consumed in the background without blocking your code
- Writes every token usage dimension, including zeros, so CLS dashboard SUM queries never come back empty
- Built-in masking hook, per-value length limit and per-span attribute byte budget; truncated JSON stays valid
- Cancellation and interruption are recorded as soft states (`eino.status`), not failures

## Installation

```bash
go env -w GOPRIVATE=git.woa.com
go get git.woa.com/albertbi/tencentcloud-agentobs-sdk-eino-go
```

```go
import agentobs "git.woa.com/albertbi/tencentcloud-agentobs-sdk-eino-go"
```

Requires Go 1.23+ and `github.com/cloudwego/eino` v0.9.x.

## Quick start

```go
handler, err := agentobs.NewHandler(ctx, &agentobs.Config{
    ServiceName: "customer-support-agent",
    Environment: "production",
})
if err != nil { /* ... */ }
defer handler.Shutdown(context.Background()) // always call before exit so data is flushed

runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent, EnableStreaming: true})

// Call StartTrace once per user request (one turn)
traceCtx := handler.StartTrace(ctx,
    agentobs.WithSessionID(conversationID),
    agentobs.WithTurnIndex(turnNo),
    agentobs.WithUserID(userID),
)
events := runner.Query(traceCtx, question, adk.WithCallbacks(handler))
for { /* consume events as usual */ }
handler.EndTrace(traceCtx, "") // with an empty string, the final answer comes from the agent output
```

For Graph/Chain, register with `compose.WithCallbacks(handler)`. You can also register globally with `callbacks.AppendGlobalHandlers(handler)`. See [`example_test.go`](example_test.go) for complete examples.

`StartTrace` is optional. Without it no entry span is created, the outermost agent becomes the root span, and the session/turn IDs default to the trace ID.

## Authentication and endpoint

| Item | Source |
| --- | --- |
| SecretId / SecretKey | `Config.SecretID/SecretKey`, otherwise the `TENCENTCLOUD_SECRET_ID` / `TENCENTCLOUD_SECRET_KEY` environment variables |
| Region | `Config.Region`, otherwise `CLS_DEFAULT_REGION`, e.g. `ap-guangzhou` |
| Trace topic | `Config.TopicID`, otherwise `CLS_TOPIC_ID` |
| Endpoint | Public `https://{region}.cls.tencentcs.com/v1/traces`; with `Intranet: true`, the VPC endpoint `{region}.cls.tencentyun.com`; or set `Endpoint` explicitly |

Requests use OTLP/HTTP protobuf with gzip compression and HTTP Basic authentication (`SecretId:SecretKey`), and carry a `topic_id` header. For cross-region uploads, set `CrossRegion` to send the `x-cross-region` header.

Security requirements:

- Never hardcode secrets. Use a CAM sub-account that only has CLS write permission, and inject its keys through environment variables or a secret manager.
- `Endpoint` accepts https only by default. http is allowed only when `Insecure: true` is set explicitly, for local debugging only. The URL must not contain a username or password.
- Configuration error messages never include secrets.

## Span hierarchy and fields

```
entry  (enter_application)   StartTrace / EndTrace                SERVER
└── agent (invoke_agent)     ADK agent, or outermost Graph/Chain  INTERNAL
    ├── step (react)         one synthesized per ReAct round      INTERNAL
    │   ├── chat (chat)      ChatModel / AgenticModel             CLIENT
    │   └── tool (execute_tool)                                   CLIENT
    │       └── agent (invoke_subagent)  agent run by the tool    INTERNAL
    └── retriever / embedding                                     CLIENT
```

| Field | Description |
| --- | --- |
| `gen_ai.span.kind` | `entry` / `agent` / `step` / `chat` / `tool`; extension values `retriever` / `embedding` / `task` |
| `gen_ai.operation.name` | `enter_application` / `invoke_agent` / `invoke_subagent` (agent nested in another agent) / `react` / `chat` / `execute_tool` / `retrieval` / `embeddings` |
| `gen_ai.session.id` | `WithSessionID`; defaults to the trace ID |
| `gen_ai.turn.id` | `WithTurnID`, or `<session>:t<n>` from `WithTurnIndex(n)`; by default equal to the trace ID when no session is set, otherwise `<session>:<traceID>` |
| `gen_ai.step.id` / `gen_ai.react.round` | `<turn>:s<n>`, and the round number within the agent (starting at 1) |
| `gen_ai.agent.id` / `gen_ai.agent.name` / `gen_ai.agent.type` | The agent's span ID, its name, and `Config.AgentType` (default `eino`) |
| `gen_ai.agent.scope` / `gen_ai.subagent.parent_tool_call.id` | On nested agents: `subagent`, and the `gen_ai.tool.call.id` of the tool that ran it (when invoked by a tool) |
| `gen_ai.user.id` / `gen_ai.user.name` / `gen_ai.entry.type` | From the `StartTrace` options; `entry.type` defaults to `sdk` |
| `gen_ai.input.messages` / `gen_ai.output.messages` | OTel GenAI message format (`parts` types: `text`, `reasoning`, `tool_call`, `tool_call_response`, `uri`, `blob`), plus `gen_ai.input.messages.hash` |
| `gen_ai.usage.*` | On chat: `input_tokens`, `output_tokens`, `total_tokens`, `cache_read.input_tokens`, `cache_miss.input_tokens`, `reasoning_output_tokens` (all 0 when a failed or cancelled call returned no usage). On agent: sum of `input_tokens`, `output_tokens`, `total_tokens`, `cache_read.input_tokens` over all model calls in the agent, including its subagents |
| `gen_ai.request.*` / `gen_ai.response.*` | model, temperature, max_tokens, top_p, stop_sequences, finish_reasons, time_to_first_token_ms. Model fields are written only after the model responds; `time_to_first_token_ms` is the first chunk time for streaming calls and the response time for non-streaming calls (not written for failed calls); `finish_reasons` is `["error"]` / `["cancelled"]` when a call fails or is cancelled without a provider reason |
| `gen_ai.chat.duration_ms` / `gen_ai.tool.call.duration_ms` | Durations |
| `gen_ai.tool.*` | `name`, `type`, `call.id`, `call.arguments`, `call.result`, `error.type`, `definitions` |
| `gen_ai.retrieval.*` | `query_text`, `top_k`, `documents`, `documents.count` (the count is written even with content capture disabled) |
| `gen_ai.react.finish_reason` | `tool_calls` / `stop` / `error` (the round's model call failed) / the raw value returned by the provider |
| `gen_ai.agent.tool_call_count` / `gen_ai.agent.message_count` | Per-agent totals |
| `error.type` / `error.message` | Written when the span status is ERROR |
| `eino.*` | Eino extensions: component type, agent source, soft status, retry events, internal callback panics, etc. |

The entry input comes from `WithInput` / `WithInputMessages`; if neither is set, the first observed user message is used. The entry output is chosen in this order: the value passed to `EndTrace`, then the outermost agent's final answer, then the last chat output. Custom attributes added with `WithAttributes` (e.g. `git.repo`) are written to every span of the trace; keys with the `gen_ai.*` or `eino.*` prefix are ignored.

### Error ownership

- Failed chat, tool and other spans are marked ERROR; when a chat fails, its step is marked ERROR too.
- The entry is marked ERROR only when the outermost agent fails, or when a component outside any agent fails. An inner agent failure that the outer agent recovers from does not affect the entry.
- `context.Canceled` is recorded as `eino.status=cancelled` and interrupt/resume as `interrupted`; neither sets the ERROR status.
- ADK `WillRetryError`s are only recorded in `eino.retry.events` and are not failures.

## Main options

| Field | Default | Description |
| --- | --- | --- |
| `DisableContentCapture` | false | Skip messages, tool arguments/results and retrieved documents; keep tokens, timings and IDs |
| `MaskFunc` | nil | Every content value passes through this function before export (applied before length limits) |
| `MaxAttributeValueLength` | 256 KiB | Length limit for each content value; negative means unlimited |
| `MaxSpanAttributeBytes` | 1,000,000 | Total attribute byte budget per span; negative means unlimited |
| `IncludeFrameworkSpans` | false | Keep framework nodes such as Lambda and ToolsNode (kind `task`); by default they are dropped and their children attach to the nearest kept parent |
| `DisableGraphAsAgent` | false | Do not treat the outermost Graph/Chain as an agent |
| `DisableStepSpans` | false | Do not emit step spans; chat/tool spans still carry the step ID and round |
| `SampleRate` | 1 | Sample by trace ID |
| `MaxQueueSize` / `MaxExportBatchSize` / `BatchTimeout` | 2048 / 512 / 5s | Export queue capacity, spans per request, maximum wait for a partial batch; the batch size must not exceed the queue size |
| `RetryConfig` | OTel defaults | OTLP retry policy (enabled, 5s initial interval, 30s max interval, 1 minute total window); a zero `MaxElapsedTime` retries forever |
| `ExportDiagnostics` | false | Add payload sizes and network-stage timings to the log of a final export failure (see below) |
| `SpanExporter` / `TracerProvider` | — | A custom exporter, or an existing provider to reuse (the handler only flushes it and never shuts it down) |

## Drop and export-failure logs

Telemetry is never discarded silently. Sampling, a full queue, a stopped processor, SDK value/attribute limits, attributes/events/links dropped by the OTel SDK, and serialization failures are summarized in one log line per `DropLogInterval` (default 1 minute; a negative value logs every event immediately):

```text
agentobs: discarded or truncated telemetry since previous report (interval 1m0s): queue_full_spans=37 value_limit_truncations=2
```

| Counter | Meaning |
| --- | --- |
| `queue_full_spans` | Spans dropped because the export queue was full; increase `MaxQueueSize` or check CLS reachability |
| `processor_stopped_spans` | Spans that ended after `Shutdown` |
| `sampled_out_spans` | Spans not sampled under `SampleRate` |
| `value_limit_truncations` / `span_attribute_budget_drops_or_truncations` | Values truncated by `MaxAttributeValueLength` / `MaxSpanAttributeBytes` |
| `otel_dropped_attributes` / `otel_dropped_events` / `otel_dropped_links` | Items dropped by the OTel SDK's own span limits |
| `payload_serialization_failures` / `metadata_serialization_failures` | Values that could not be serialized to JSON |

When the exporter finally gives up on a batch, the number of spans in it is logged right away, because the batch will not be retried. With `ExportDiagnostics` enabled, that log line also includes the protobuf and gzip body sizes, the number of HTTP attempts, and each attempt's status code plus DNS, connect, TLS, request-write and wait-for-headers timings:

```text
agentobs: CLS export failed for batch of 12 spans; failed or rejected spans will not be retried by the processor: <exporter error>; export diagnostics: protobuf_bytes=48213 gzip_bytes=9120 attempts=3 attempt_1={status=503 reused=false total=1.2s dns=3ms connect=8ms tls=21ms write=1ms wait_headers=1.1s} ...
```

The diagnostics never include request headers (credentials), span contents or trace IDs. Only the first request body is re-read for sizing, which adds a small CPU and memory cost.

With an external `TracerProvider`, queueing and export are handled by that provider's own span processor, so these processor counters and diagnostics do not apply; the SDK's truncation and serialization counters still do.

## Known limitations

- When a model does not implement callbacks itself, the callbacks Eino injects for it carry only the messages. Parameters such as `gen_ai.request.model`, `gen_ai.tool.definitions` and temperature require the model component to report `model.CallbackInput` itself. Model implementations in eino-ext generally do; check your custom models.
- `WithTraceID` requires the provider the handler builds itself. With an external `TracerProvider`, the trace ID is decided by that provider's IDGenerator.
- The agent span ends only after the caller has fully consumed the ADK event iterator.

## License

Apache License 2.0. The callback lifecycle, ADK event handling, error ownership and attribute limits are adapted from [cloudwego/eino-ext `callbacks/langfuse/v2`](https://github.com/cloudwego/eino-ext). See [NOTICE](NOTICE).
