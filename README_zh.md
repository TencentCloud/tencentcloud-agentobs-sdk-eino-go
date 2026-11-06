# tencentcloud-agentobs-sdk-eino-go

[English](README.md) | 中文

把 [CloudWeGo Eino](https://github.com/cloudwego/eino) 的回调转换为符合**腾讯云 CLS Agent Trace 字段规范**的 OpenTelemetry Trace，通过 OTLP/HTTP 上报到 CLS（日志服务）的 Agent 可观测能力。

- 支持 ADK Agent（`ChatModelAgent`、Agentic Agent、嵌套 AgentTool）、Graph / Chain / Workflow、ChatModel / AgenticModel、Tool、Retriever、Embedding
- 自动合成 ReAct `step` span：每轮模型调用对应一个 step，工具按 `tool_call_id` 归属到发起它的那一轮
- 支持流式输入/输出（记录 TTFT），流在后台消费，不阻塞业务
- token 用量各维度都会写出（包括 0 值），方便 CLS 仪表盘做 SUM 聚合
- 内置脱敏钩子、单值长度上限、单 span 属性字节预算；截断后的 JSON 仍然合法
- 取消和中断记为软状态（`eino.status`），不算失败

## 安装

```bash
go env -w GOPRIVATE=git.woa.com
go get git.woa.com/albertbi/tencentcloud-agentobs-sdk-eino-go
```

```go
import agentobs "git.woa.com/albertbi/tencentcloud-agentobs-sdk-eino-go"
```

依赖 Go 1.23+ 和 `github.com/cloudwego/eino` v0.9.x。

## 快速开始

```go
handler, err := agentobs.NewHandler(ctx, &agentobs.Config{
    ServiceName: "customer-support-agent",
    Environment: "production",
})
if err != nil { /* ... */ }
defer handler.Shutdown(context.Background()) // 退出前务必调用，确保数据发出

runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent, EnableStreaming: true})

// 每个用户请求（一个 turn）调用一次 StartTrace
traceCtx := handler.StartTrace(ctx,
    agentobs.WithSessionID(conversationID),
    agentobs.WithTurnIndex(turnNo),
    agentobs.WithUserID(userID),
)
events := runner.Query(traceCtx, question, adk.WithCallbacks(handler))
for { /* 正常消费事件 */ }
handler.EndTrace(traceCtx, "") // 传空串时，最终回答取 agent 的输出
```

Graph/Chain 用 `compose.WithCallbacks(handler)` 注册即可；也可以调用 `callbacks.AppendGlobalHandlers(handler)` 做全局注册。完整示例见 [`example_test.go`](example_test.go)。

不调用 `StartTrace` 也能用：这时不生成 entry span，最外层 agent 就是根 span，session/turn ID 默认取 trace ID。

## 鉴权与上报地址

| 项 | 来源 |
| --- | --- |
| SecretId / SecretKey | `Config.SecretID/SecretKey`，否则读环境变量 `TENCENTCLOUD_SECRET_ID` / `TENCENTCLOUD_SECRET_KEY` |
| 地域 | `Config.Region`，否则读 `CLS_DEFAULT_REGION`，例如 `ap-guangzhou` |
| Trace 主题 | `Config.TopicID`，否则读 `CLS_TOPIC_ID` |
| 上报地址 | 公网 `https://{region}.cls.tencentcs.com/v1/traces`；设置 `Intranet: true` 时走内网 `{region}.cls.tencentyun.com`；也可用 `Endpoint` 指定 |

请求格式为 OTLP/HTTP protobuf，gzip 压缩，HTTP Basic 鉴权（`SecretId:SecretKey`），并携带 `topic_id` 请求头。跨地域上报时设置 `CrossRegion`，会写入 `x-cross-region` 请求头。

安全要求：

- 不要把密钥写进代码。请使用只授予 CLS 写权限的 CAM 子账号，通过环境变量或密钥管理系统注入密钥。
- `Endpoint` 默认只接受 https。只有显式设置 `Insecure: true` 才允许 http，仅限本地调试。URL 中不能带用户名密码。
- 配置错误返回的错误信息里不会出现密钥。

## Span 层级与字段

```
entry  (enter_application)   StartTrace / EndTrace              SERVER
└── agent (invoke_agent)     ADK Agent，或最外层 Graph/Chain    INTERNAL
    ├── step (react)         每轮 ReAct 合成一个                INTERNAL
    │   ├── chat (chat)      ChatModel / AgenticModel           CLIENT
    │   └── tool (execute_tool)                                 CLIENT
    │       └── agent (invoke_subagent)  由 tool 调起的子 agent  INTERNAL
    └── retriever / embedding                                   CLIENT
```

| 字段 | 说明 |
| --- | --- |
| `gen_ai.span.kind` | `entry` / `agent` / `step` / `chat` / `tool`；扩展值有 `retriever` / `embedding` / `task` |
| `gen_ai.operation.name` | `enter_application` / `invoke_agent` / `invoke_subagent`（嵌套在其他 agent 内的 agent）/ `react` / `chat` / `execute_tool` / `retrieval` / `embeddings` |
| `gen_ai.session.id` | `WithSessionID`，默认取 trace ID |
| `gen_ai.turn.id` | `WithTurnID`；或由 `WithTurnIndex(n)` 生成 `<session>:t<n>`；默认：未设置 session 时等于 trace ID，否则为 `<session>:<traceID>` |
| `gen_ai.step.id` / `gen_ai.react.round` | `<turn>:s<n>`，以及该 agent 内的轮次（从 1 开始） |
| `gen_ai.agent.id` / `gen_ai.agent.name` / `gen_ai.agent.type` | agent 的 span ID、名称、`Config.AgentType`（默认 `eino`） |
| `gen_ai.agent.scope` / `gen_ai.subagent.parent_tool_call.id` | 子 agent 上写入：`subagent`，以及调起它的 tool 的 `gen_ai.tool.call.id`（由 tool 调起时） |
| `gen_ai.user.id` / `gen_ai.user.name` / `gen_ai.entry.type` | 来自 `StartTrace` 选项，`entry.type` 默认 `sdk` |
| `gen_ai.input.messages` / `gen_ai.output.messages` | OTel GenAI 消息格式（`parts` 支持 `text`、`reasoning`、`tool_call`、`tool_call_response`、`uri`、`blob`），附 `gen_ai.input.messages.hash` |
| `gen_ai.usage.*` | chat 上：`input_tokens`、`output_tokens`、`total_tokens`、`cache_read.input_tokens`、`cache_miss.input_tokens`、`reasoning_output_tokens`（调用失败或取消且没有返回用量时全部写 0）。agent 上：该 agent 内所有模型调用（含子 agent）的 `input_tokens`、`output_tokens`、`total_tokens`、`cache_read.input_tokens` 之和 |
| `gen_ai.request.*` / `gen_ai.response.*` | model、temperature、max_tokens、top_p、stop_sequences、finish_reasons、time_to_first_token_ms。模型字段只在拿到模型响应后写入；`time_to_first_token_ms` 流式调用取首个 chunk 的时间，非流式调用取拿到响应的时间（失败的调用不写）；调用失败或取消、且 provider 没给出原因时，`finish_reasons` 为 `["error"]` / `["cancelled"]` |
| `gen_ai.chat.duration_ms` / `gen_ai.tool.call.duration_ms` | 耗时 |
| `gen_ai.tool.*` | `name`、`type`、`call.id`、`call.arguments`、`call.result`、`error.type`、`definitions` |
| `gen_ai.retrieval.*` | `query_text`、`top_k`、`documents`、`documents.count`（关闭内容采集时仍写入数量） |
| `gen_ai.react.finish_reason` | `tool_calls` / `stop` / `error`（本轮模型调用失败）/ provider 返回的原值 |
| `gen_ai.agent.tool_call_count` / `gen_ai.agent.message_count` | agent 级汇总 |
| `error.type` / `error.message` | span 状态为 ERROR 时写入 |
| `eino.*` | Eino 扩展字段：组件类型、agent 来源、软状态、重试事件、回调内部异常等 |

entry 的输入取 `WithInput` / `WithInputMessages`，没有设置时取首次观察到的 user 消息。输出按优先级取：`EndTrace` 传入的值 > 最外层 agent 的最终回答 > 最后一次 chat 输出。`WithAttributes` 添加的自定义字段（例如 `git.repo`）会写到该 trace 的所有 span 上；`gen_ai.*` 和 `eino.*` 前缀的键会被忽略。

### 错误归属

- 失败的 chat、tool 等 span 标为 ERROR；chat 失败时，它所在的 step 也标为 ERROR。
- 只有最外层 agent 失败，或者 agent 之外的组件失败，entry 才会标为 ERROR。内层 agent 失败后如果被外层恢复，不影响 entry。
- `context.Canceled` 记为 `eino.status=cancelled`，中断/恢复（interrupt）记为 `interrupted`，这两种都不设 ERROR 状态。
- ADK 的 `WillRetryError` 只记录在 `eino.retry.events` 中，不算失败。

## 主要配置

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `DisableContentCapture` | false | 不采集消息、工具参数/结果和检索文档，只保留 token、耗时和 ID |
| `MaskFunc` | nil | 所有内容在导出前先经过这个函数（在长度限制之前执行） |
| `MaxAttributeValueLength` | 256 KiB | 单个内容值的长度上限，负数表示不限制 |
| `MaxSpanAttributeBytes` | 1,000,000 | 单个 span 的属性总字节预算，负数表示不限制 |
| `IncludeFrameworkSpans` | false | 保留 Lambda、ToolsNode 等框架节点（kind 为 `task`）；默认丢弃，其子 span 挂到最近一个保留的父 span 上 |
| `DisableGraphAsAgent` | false | 不把最外层 Graph/Chain 当作 agent |
| `DisableStepSpans` | false | 不生成 step span，但 chat/tool 上仍保留 step ID 和轮次 |
| `SampleRate` | 1 | 按 trace ID 采样 |
| `MaxQueueSize` / `MaxExportBatchSize` / `BatchTimeout` | 2048 / 512 / 5s | 待上报队列容量、每次请求的 span 数、未满一批时最长等待时间；每批数量不能超过队列容量 |
| `RetryConfig` | OTel 默认 | OTLP 重试策略（默认开启：首次间隔 5 秒，最大间隔 30 秒，总重试窗口 1 分钟）；`MaxElapsedTime` 为 0 表示无限重试 |
| `ExportDiagnostics` | false | 最终导出失败时，日志里额外记录请求体大小和各网络阶段耗时（见下文） |
| `SpanExporter` / `TracerProvider` | — | 自定义导出器，或复用已有的 provider（handler 只 flush，不负责关闭它） |

## 丢弃与导出失败日志

遥测数据不会被悄悄丢掉。采样、队列已满、处理器已关闭、SDK 的长度/字节预算限制、OTel SDK 自身丢弃的属性/事件/链接，以及序列化失败，都会按 `DropLogInterval`（默认 1 分钟；设为负数则每次发生立即记录）汇总成一行日志：

```text
agentobs: discarded or truncated telemetry since previous report (interval 1m0s): queue_full_spans=37 value_limit_truncations=2
```

| 计数项 | 含义 |
| --- | --- |
| `queue_full_spans` | 上报队列已满被丢弃的 span；可以调大 `MaxQueueSize`，或检查到 CLS 的网络是否通畅 |
| `processor_stopped_spans` | 在 `Shutdown` 之后才结束的 span |
| `sampled_out_spans` | 按 `SampleRate` 未被采样的 span |
| `value_limit_truncations` / `span_attribute_budget_drops_or_truncations` | 因 `MaxAttributeValueLength` / `MaxSpanAttributeBytes` 被截断的值 |
| `otel_dropped_attributes` / `otel_dropped_events` / `otel_dropped_links` | OTel SDK 自身 span 限制丢弃的条目 |
| `payload_serialization_failures` / `metadata_serialization_failures` | 无法序列化为 JSON 的值 |

导出器对一批数据最终放弃时，会立即打印这一批的 span 数量，因为这批数据不会再重试。开启 `ExportDiagnostics` 后，这条日志还会附带 protobuf 和 gzip 请求体大小、HTTP 尝试次数，以及每次尝试的状态码和 DNS、建连、TLS、写请求、等待响应头的耗时：

```text
agentobs: CLS export failed for batch of 12 spans; failed or rejected spans will not be retried by the processor: <exporter error>; export diagnostics: protobuf_bytes=48213 gzip_bytes=9120 attempts=3 attempt_1={status=503 reused=false total=1.2s dns=3ms connect=8ms tls=21ms write=1ms wait_headers=1.1s} ...
```

诊断信息不会包含请求头（即凭证）、span 内容或 trace ID。为了统计大小，只会重新读取第一次的请求体，会带来少量 CPU 和内存开销。

使用外部 `TracerProvider` 时，排队和导出由该 provider 自己的 span processor 负责，上面的处理器计数和诊断不生效；SDK 自身的截断和序列化计数仍然有效。

## 已知限制

- Eino 为未实现回调的模型自动注入的回调只带 messages。`gen_ai.request.model`、`gen_ai.tool.definitions` 和 temperature 等参数，需要模型组件自己上报 `model.CallbackInput`。eino-ext 中的模型实现一般会上报，自定义模型请自行确认。
- `WithTraceID` 依赖 handler 自建的 provider。使用外部 `TracerProvider` 时，trace ID 由该 provider 的 IDGenerator 决定。
- 调用方必须把 ADK 事件迭代器消费完，agent span 才会结束。

## 许可

Apache License 2.0。回调生命周期、ADK 事件处理、错误归属和属性限制这几部分改编自 [cloudwego/eino-ext `callbacks/langfuse/v2`](https://github.com/cloudwego/eino-ext)，详见 [NOTICE](NOTICE)。
