package agentobs

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestTruncateJSONStaysValid(t *testing.T) {
	value, _ := json.Marshal(map[string]string{"content": strings.Repeat("数据", 500)})
	for _, limit := range []int{2, 20, 64, 200, 1000} {
		got := truncateValue(string(value), limit)
		if len(got) > limit {
			t.Errorf("limit %d: got %d bytes", limit, len(got))
		}
		if !json.Valid([]byte(got)) {
			t.Errorf("limit %d: invalid JSON %q", limit, got)
		}
	}
	var envelope map[string]any
	_ = json.Unmarshal([]byte(truncateValue(string(value), 1000)), &envelope)
	if envelope["truncated"] != true || int(envelope["original_bytes"].(float64)) != len(value) {
		t.Errorf("envelope = %v", envelope)
	}
}

func TestTruncateTextKeepsHeadTailAndUTF8(t *testing.T) {
	value := "开头" + strings.Repeat("中间内容", 200) + "结尾"
	got := truncateText(value, 100)
	if len(got) > 100 || !utf8.ValidString(got) {
		t.Fatalf("got %d bytes, valid=%v", len(got), utf8.ValidString(got))
	}
	if !strings.HasPrefix(got, "开头") || !strings.HasSuffix(got, "结尾") || !strings.Contains(got, "[truncated]") {
		t.Errorf("got %q", got)
	}
	if got := utf8Prefix("ab\xffcd", 4); got != "ab\xffc" {
		t.Errorf("invalid input bytes must not empty the prefix: %q", got)
	}
}

func TestSpanAttributeBudget(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	_, span := provider.Tracer("t").Start(context.Background(), "s")
	losses := newLossReporter(time.Hour, nil)
	defer losses.Stop()
	writer := newSpanAttributeWriter(span, func(v string) string { return v }, 100, losses)
	writer.setString("a", strings.Repeat("x", 60))
	written := writer.setJSONContent("b", map[string]string{"k": strings.Repeat("y", 100)})
	writer.setInt("c", 12345)
	span.End()

	if len(written) > 100-61-1 || !json.Valid([]byte(written)) {
		t.Errorf("budgeted JSON = %q", written)
	}
	stub := exporter.GetSpans()[0]
	total := 0
	for _, kv := range stub.Attributes {
		total += len(kv.Key) + len(kv.Value.Emit())
	}
	if total > 100 {
		t.Errorf("span attributes use %d bytes", total)
	}
}

func TestValuePreparerMasksBeforeLimit(t *testing.T) {
	losses := newLossReporter(time.Hour, nil)
	defer losses.Stop()
	prepare := newValuePreparer(func(v string) string { return strings.ReplaceAll(v, "secret", "******") }, 30, losses)
	got := prepare("token=secret " + strings.Repeat("z", 100))
	if strings.Contains(got, "secret") || len(got) > 30 {
		t.Errorf("got %q", got)
	}
}

func TestConvertMessages(t *testing.T) {
	url := "https://example.com/cat.png"
	inline := "data:image/png;base64,iVBORw0KGgo="
	messages := convertMessages([]*schema.Message{
		schema.SystemMessage("be nice"),
		{
			Role: schema.User,
			UserInputMultiContent: []schema.MessageInputPart{
				{Type: schema.ChatMessagePartTypeText, Text: "look"},
				{Type: schema.ChatMessagePartTypeImageURL, Image: &schema.MessageInputImage{MessagePartCommon: schema.MessagePartCommon{URL: &url, MIMEType: "image/png"}}},
				{Type: schema.ChatMessagePartTypeImageURL, Image: &schema.MessageInputImage{MessagePartCommon: schema.MessagePartCommon{URL: &inline}}},
			},
		},
		{
			Role:             schema.Assistant,
			ReasoningContent: "thinking",
			ToolCalls:        []schema.ToolCall{{ID: "c1", Function: schema.FunctionCall{Name: "f", Arguments: `{"x":1}`}}},
			ResponseMeta:     &schema.ResponseMeta{FinishReason: "tool_calls"},
		},
		schema.ToolMessage(`{"ok":true}`, "c1", schema.WithToolName("f")),
		nil,
	})
	encoded, _ := json.Marshal(messages)
	text := string(encoded)
	for _, want := range []string{
		`{"role":"system","parts":[{"type":"text","content":"be nice"}]}`,
		`{"type":"uri","modality":"image","mime_type":"image/png","uri":"https://example.com/cat.png"}`,
		`{"type":"blob","modality":"image"}`,
		`{"type":"reasoning","content":"thinking"}`,
		`{"type":"tool_call","id":"c1","name":"f","arguments":{"x":1}}`,
		`"finish_reason":"tool_calls"`,
		`{"role":"tool","parts":[{"type":"tool_call_response","id":"c1","response":{"ok":true}}],"name":"f"}`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %s in %s", want, text)
		}
	}
	if strings.Contains(text, "iVBORw0KGgo") {
		t.Errorf("inline base64 copied: %s", text)
	}
	if len(messages) != 4 {
		t.Errorf("nil message not skipped")
	}
}

func TestConvertAgenticMessages(t *testing.T) {
	messages := convertAgenticMessages([]*schema.AgenticMessage{
		{Role: schema.AgenticRoleTypeUser, ContentBlocks: []*schema.ContentBlock{schema.NewContentBlock(&schema.UserInputText{Text: "hi"})}},
		{Role: schema.AgenticRoleTypeAssistant, ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.Reasoning{Text: "hmm"}),
			schema.NewContentBlock(&schema.FunctionToolCall{CallID: "c9", Name: "search", Arguments: `{"q":"eino"}`}),
		}},
		{Role: schema.AgenticRoleTypeAssistant, ContentBlocks: []*schema.ContentBlock{schema.NewContentBlock(&schema.AssistantGenText{Text: "done"})}},
	})
	if len(messages) != 3 {
		t.Fatalf("messages = %+v", messages)
	}
	if messages[1].FinishReason != "tool_call" || reactFinishReason(messages[1]) != "tool_calls" || toolCallIDs(messages[1])[0] != "c9" {
		t.Errorf("tool call message = %+v", messages[1])
	}
	if messages[2].FinishReason != "stop" || reactFinishReason(messages[2]) != "stop" {
		t.Errorf("final message = %+v", messages[2])
	}
	final := finalAssistantMessage(messages)
	if len(final) != 1 || final[0].Parts[0].Content != "done" {
		t.Errorf("final = %+v", final)
	}
	if user := lastUserMessage(messages); len(user) != 1 || user[0].Parts[0].Content != "hi" {
		t.Errorf("last user = %+v", user)
	}
}

func TestReactFinishReasonNormalization(t *testing.T) {
	for input, want := range map[string]string{"": "stop", "end_turn": "stop", "tool_use": "tool_calls", "length": "length", "STOP": "stop"} {
		if got := reactFinishReason(genAIMessage{FinishReason: input}); got != want {
			t.Errorf("%q -> %q, want %q", input, got, want)
		}
	}
}

func TestSetUsageClampsDetails(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	_, span := provider.Tracer("t").Start(context.Background(), "s")
	losses := newLossReporter(time.Hour, nil)
	defer losses.Stop()
	writer := newSpanAttributeWriter(span, func(v string) string { return v }, -1, losses)
	setUsage(writer, &schema.TokenUsage{
		PromptTokens: 10, CompletionTokens: 5,
		PromptTokenDetails:      schema.PromptTokenDetails{CachedTokens: 50},
		CompletionTokensDetails: schema.CompletionTokensDetails{ReasoningTokens: -1},
	})
	span.End()
	stub := exporter.GetSpans()[0]
	for key, want := range map[string]string{
		attrUsageTotalTokens: "15", attrUsageCacheReadTokens: "10", attrUsageCacheMissTokens: "0", attrUsageReasoningTokens: "0",
	} {
		assertAttr(t, stub, key, want)
	}
}
