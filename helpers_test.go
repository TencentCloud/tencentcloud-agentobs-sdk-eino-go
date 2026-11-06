package agentobs

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/components/tool/utils"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type testEnv struct {
	t        *testing.T
	handler  *Handler
	exporter *tracetest.InMemoryExporter
}

func newTestEnv(t *testing.T, mutate func(*Config)) *testEnv {
	t.Helper()
	exporter := tracetest.NewInMemoryExporter()
	cfg := &Config{
		SpanExporter:    exporter,
		ServiceName:     "agentobs-test",
		BatchTimeout:    10 * time.Millisecond,
		DropLogInterval: time.Hour,
	}
	if mutate != nil {
		mutate(cfg)
	}
	handler, err := NewHandler(context.Background(), cfg)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = handler.Shutdown(ctx)
	})
	return &testEnv{t: t, handler: handler, exporter: exporter}
}

func (e *testEnv) spans() spanSet {
	e.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := e.handler.Flush(ctx); err != nil {
		e.t.Fatalf("Flush: %v", err)
	}
	return spanSet(e.exporter.GetSpans())
}

type spanSet tracetest.SpanStubs

func (s spanSet) byKind(kind string) []tracetest.SpanStub {
	var result []tracetest.SpanStub
	for _, span := range s {
		if attr(span, attrSpanKind) == kind {
			result = append(result, span)
		}
	}
	return result
}

func (s spanSet) one(t *testing.T, kind string) tracetest.SpanStub {
	t.Helper()
	found := s.byKind(kind)
	if len(found) != 1 {
		t.Fatalf("want 1 %s span, got %d (%s)", kind, len(found), s.describe())
	}
	return found[0]
}

func (s spanSet) describe() string {
	names := make([]string, 0, len(s))
	for _, span := range s {
		names = append(names, attr(span, attrSpanKind)+":"+span.Name)
	}
	return strings.Join(names, ", ")
}

func attr(span tracetest.SpanStub, key string) string {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value.Emit()
		}
	}
	return ""
}

func hasAttr(span tracetest.SpanStub, key string) bool {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return true
		}
	}
	return false
}

func attrValue(span tracetest.SpanStub, key string) (attribute.Value, bool) {
	for _, kv := range span.Attributes {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func assertAttr(t *testing.T, span tracetest.SpanStub, key, want string) {
	t.Helper()
	if got := attr(span, key); got != want {
		t.Errorf("%s %s = %q, want %q", span.Name, key, got, want)
	}
}

func assertParent(t *testing.T, child, parent tracetest.SpanStub) {
	t.Helper()
	if child.Parent.SpanID() != parent.SpanContext.SpanID() {
		t.Errorf("%s parent = %s, want %s (%s)", child.Name, child.Parent.SpanID(), parent.SpanContext.SpanID(), parent.Name)
	}
	if child.SpanContext.TraceID() != parent.SpanContext.TraceID() {
		t.Errorf("%s trace = %s, want %s", child.Name, child.SpanContext.TraceID(), parent.SpanContext.TraceID())
	}
}

func assertStatus(t *testing.T, span tracetest.SpanStub, want codes.Code) {
	t.Helper()
	if span.Status.Code != want {
		t.Errorf("%s status = %v (%q), want %v", span.Name, span.Status.Code, span.Status.Description, want)
	}
}

// scriptedModel returns the configured responses in order.
type scriptedModel struct {
	mu        sync.Mutex
	responses []*schema.Message
	errs      []error
	calls     int
}

func (m *scriptedModel) next() (*schema.Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	index := m.calls
	m.calls++
	if index < len(m.errs) && m.errs[index] != nil {
		return nil, m.errs[index]
	}
	if index >= len(m.responses) {
		return nil, errors.New("scripted model exhausted")
	}
	return m.responses[index], nil
}

func (m *scriptedModel) Generate(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.Message, error) {
	return m.next()
}

func (m *scriptedModel) Stream(_ context.Context, _ []*schema.Message, _ ...model.Option) (*schema.StreamReader[*schema.Message], error) {
	message, err := m.next()
	if err != nil {
		return nil, err
	}
	chunks := splitMessage(message)
	reader, writer := schema.Pipe[*schema.Message](len(chunks))
	go func() {
		defer writer.Close()
		for _, chunk := range chunks {
			writer.Send(chunk, nil)
		}
	}()
	return reader, nil
}

func (m *scriptedModel) WithTools([]*schema.ToolInfo) (model.ToolCallingChatModel, error) {
	return m, nil
}

// splitMessage turns a text answer into two chunks; usage and finish reason
// travel on the last chunk as real providers do.
func splitMessage(message *schema.Message) []*schema.Message {
	if len(message.ToolCalls) > 0 || len(message.Content) < 2 {
		return []*schema.Message{message}
	}
	half := len(message.Content) / 2
	first := &schema.Message{Role: message.Role, Content: message.Content[:half]}
	last := &schema.Message{Role: message.Role, Content: message.Content[half:], ResponseMeta: message.ResponseMeta}
	return []*schema.Message{first, last}
}

func toolCallMessage(id, name, arguments string, usage *schema.TokenUsage) *schema.Message {
	return &schema.Message{
		Role: schema.Assistant,
		ToolCalls: []schema.ToolCall{{
			ID:       id,
			Type:     "function",
			Function: schema.FunctionCall{Name: name, Arguments: arguments},
		}},
		ResponseMeta: &schema.ResponseMeta{FinishReason: "tool_calls", Usage: usage},
	}
}

func answerMessage(content string, usage *schema.TokenUsage) *schema.Message {
	return &schema.Message{
		Role:         schema.Assistant,
		Content:      content,
		ResponseMeta: &schema.ResponseMeta{FinishReason: "stop", Usage: usage},
	}
}

type weatherInput struct {
	City string `json:"city" jsonschema:"description=city name"`
}

func weatherTool(t *testing.T, fn func(context.Context, weatherInput) (string, error)) tool.BaseTool {
	t.Helper()
	if fn == nil {
		fn = func(_ context.Context, in weatherInput) (string, error) {
			return `{"city":"` + in.City + `","weather":"sunny"}`, nil
		}
	}
	weather, err := utils.InferTool("get_weather", "Query the weather of a city", fn)
	if err != nil {
		t.Fatalf("InferTool: %v", err)
	}
	return weather
}

func newTestAgent(t *testing.T, name string, chatModel model.ToolCallingChatModel, tools ...tool.BaseTool) adk.Agent {
	t.Helper()
	config := &adk.ChatModelAgentConfig{
		Name:        name,
		Description: "test agent " + name,
		Instruction: "You are a helpful assistant.",
		Model:       chatModel,
	}
	if len(tools) > 0 {
		config.ToolsConfig = adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: tools}}
	}
	agent, err := adk.NewChatModelAgent(context.Background(), config)
	if err != nil {
		t.Fatalf("NewChatModelAgent: %v", err)
	}
	return agent
}

// drain consumes every event (including message streams) and returns the
// last error event.
func drain(t *testing.T, events *adk.AsyncIterator[*adk.AgentEvent]) error {
	t.Helper()
	var lastErr error
	for {
		event, ok := events.Next()
		if !ok {
			return lastErr
		}
		if event.Err != nil {
			lastErr = event.Err
			continue
		}
		if event.Output != nil && event.Output.MessageOutput != nil {
			if stream := event.Output.MessageOutput.MessageStream; stream != nil {
				for {
					if _, err := stream.Recv(); err != nil {
						if !errors.Is(err, io.EOF) {
							lastErr = err
						}
						break
					}
				}
				stream.Close()
			}
		}
	}
}
