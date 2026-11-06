// Package agentobs exports CloudWeGo Eino callbacks to Tencent Cloud CLS
// (Cloud Log Service) Agent Observability as OpenTelemetry traces that follow
// the CLS Agent Trace field specification.
//
// Every Eino run is mapped onto the CLS span hierarchy:
//
//	entry (enter_application)          StartTrace / EndTrace
//	└── agent (invoke_agent)           ADK Agent, or the outermost Graph/Chain
//	    └── step (react)               synthesized per ReAct round
//	        ├── chat (chat)            ChatModel / AgenticModel
//	        └── tool (execute_tool)    Tool
//	            └── agent (invoke_subagent)  agent run by the tool
//
// Spans are exported with OTLP/HTTP (protobuf + gzip) to
// https://{region}.cls.tencentcs.com/v1/traces using HTTP Basic authentication
// with a Tencent Cloud SecretId/SecretKey and the topic_id header.
//
// The callback lifecycle, ADK agent event handling, error ownership and
// attribute-limit logic are adapted from
// github.com/cloudwego/eino-ext/callbacks/langfuse/v2 (Apache-2.0).
package agentobs
