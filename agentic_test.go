package agentobs

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/tool"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type scriptedAgenticModel struct {
	mu        sync.Mutex
	responses []*schema.AgenticMessage
	calls     int
}

func (m *scriptedAgenticModel) next() (*schema.AgenticMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.calls >= len(m.responses) {
		return nil, errors.New("scripted agentic model exhausted")
	}
	m.calls++
	return m.responses[m.calls-1], nil
}

func (m *scriptedAgenticModel) Generate(_ context.Context, _ []*schema.AgenticMessage, _ ...model.Option) (*schema.AgenticMessage, error) {
	return m.next()
}

func (m *scriptedAgenticModel) Stream(_ context.Context, _ []*schema.AgenticMessage, _ ...model.Option) (*schema.StreamReader[*schema.AgenticMessage], error) {
	message, err := m.next()
	if err != nil {
		return nil, err
	}
	return schema.StreamReaderFromArray([]*schema.AgenticMessage{message}), nil
}

func TestAgenticAgentHierarchy(t *testing.T) {
	env := newTestEnv(t, nil)
	agenticModel := &scriptedAgenticModel{responses: []*schema.AgenticMessage{
		{Role: schema.AgenticRoleTypeAssistant, ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.FunctionToolCall{CallID: "call_a", Name: "get_weather", Arguments: `{"city":"Beijing"}`}),
		}},
		{Role: schema.AgenticRoleTypeAssistant, ContentBlocks: []*schema.ContentBlock{
			schema.NewContentBlock(&schema.AssistantGenText{Text: "Beijing is sunny."}),
		}},
	}}
	agent, err := adk.NewTypedChatModelAgent(context.Background(), &adk.TypedChatModelAgentConfig[*schema.AgenticMessage]{
		Name:        "agentic",
		Description: "agentic test agent",
		Model:       agenticModel,
		ToolsConfig: adk.ToolsConfig{ToolsNodeConfig: compose.ToolsNodeConfig{Tools: []tool.BaseTool{weatherTool(t, nil)}}},
	})
	if err != nil {
		t.Fatalf("NewTypedChatModelAgent: %v", err)
	}
	runner := adk.NewTypedRunner(adk.TypedRunnerConfig[*schema.AgenticMessage]{Agent: agent})
	ctx := env.handler.StartTrace(context.Background())
	events := runner.Query(ctx, "weather in Beijing?", adk.WithCallbacks(env.handler))
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			t.Fatalf("agent error: %v", event.Err)
		}
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()

	agentSpan := spans.one(t, SpanKindAgent)
	assertAttr(t, agentSpan, attrEinoComponent, string(adk.ComponentOfAgenticAgent))
	steps := spans.byKind(SpanKindStep)
	chats := spans.byKind(SpanKindChat)
	if len(steps) != 2 || len(chats) != 2 {
		t.Fatalf("want 2 steps/chats, got %s", spans.describe())
	}
	sortByStart(steps)
	sortByStart(chats)
	toolSpan := spans.one(t, SpanKindTool)
	assertParent(t, toolSpan, steps[0])
	assertAttr(t, toolSpan, attrToolCallID, "call_a")
	assertAttr(t, steps[0], attrReactFinishReason, "tool_calls")
	assertAttr(t, chats[0], attrEinoComponent, string(components.ComponentOfAgenticModel))
	if !strings.Contains(attr(chats[0], attrOutputMessages), `"type":"tool_call"`) {
		t.Errorf("agentic chat output = %s", attr(chats[0], attrOutputMessages))
	}
	assertOutputText(t, agentSpan, "Beijing is sunny.")
	assertOutputText(t, spans.one(t, SpanKindEntry), "Beijing is sunny.")
}

func TestAgenticModelStreamCallbacks(t *testing.T) {
	env := newTestEnv(t, nil)
	h := env.handler
	info := &callbacks.RunInfo{Name: "AgenticModel", Type: "Ark", Component: components.ComponentOfAgenticModel}
	input := schema.StreamReaderFromArray([]callbacks.CallbackInput{
		[]*schema.AgenticMessage{{Role: schema.AgenticRoleTypeUser, ContentBlocks: []*schema.ContentBlock{schema.NewContentBlock(&schema.UserInputText{Text: "hello"})}}},
	})
	ctx := h.OnStartWithStreamInput(context.Background(), info, input)
	output := schema.StreamReaderFromArray([]callbacks.CallbackOutput{
		&model.AgenticCallbackOutput{
			Message: &schema.AgenticMessage{Role: schema.AgenticRoleTypeAssistant, ContentBlocks: []*schema.ContentBlock{
				schema.NewContentBlock(&schema.AssistantGenText{Text: "hi there"}),
			}},
			Config:     &model.AgenticConfig{Model: "doubao-seed"},
			TokenUsage: &model.TokenUsage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
		},
	})
	h.OnEndWithStreamOutput(ctx, info, output)
	chat := env.spans().one(t, SpanKindChat)
	assertAttr(t, chat, attrResponseModel, "doubao-seed")
	assertAttr(t, chat, attrRequestModel, "doubao-seed")
	assertAttr(t, chat, attrUsageTotalTokens, "5")
	assertOutputText(t, chat, "hi there")
	if !strings.Contains(attr(chat, attrInputMessages), "hello") {
		t.Errorf("input = %s", attr(chat, attrInputMessages))
	}
}

func TestCallbackPanicIsContained(t *testing.T) {
	env := newTestEnv(t, func(c *Config) {
		c.MaskFunc = func(value string) string {
			if strings.Contains(value, "boom") {
				panic("mask exploded")
			}
			return value
		}
	})
	h := env.handler
	info := &callbacks.RunInfo{Name: "ChatModel", Component: components.ComponentOfChatModel}
	ctx := h.StartTrace(context.Background())
	ctx = h.OnStart(ctx, info, []*schema.Message{schema.UserMessage("boom")})
	h.OnEnd(ctx, info, answerMessage("boom", nil))
	h.EndTrace(ctx, "")
	spans := env.spans()
	chat := spans.one(t, SpanKindChat)
	if !strings.Contains(attr(chat, attrEinoCallbackPanic), "mask exploded") {
		t.Errorf("callback panic not recorded: %v", chat.Attributes)
	}
	assertStatus(t, spans.one(t, SpanKindEntry), codes.Ok)
}

func TestShutdownEndsActiveTraces(t *testing.T) {
	// InMemoryExporter.Shutdown drops recorded spans, so supply an external
	// provider that the handler only flushes.
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	env := newTestEnv(t, func(c *Config) { c.TracerProvider = provider })
	env.handler.StartTrace(context.Background(), WithTurnID("custom-turn"), WithUserName("alice"),
		WithEntryType("cli"), WithAgentType("my-agent"),
		WithInputMessages(schema.UserMessage("structured input")))
	if len(exporter.GetSpans()) != 0 {
		t.Fatal("entry ended before Shutdown")
	}
	if err := env.handler.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	spans := spanSet(exporter.GetSpans())
	entry := spans.one(t, SpanKindEntry)
	assertAttr(t, entry, attrTurnID, "custom-turn")
	assertAttr(t, entry, attrUserName, "alice")
	assertAttr(t, entry, attrEntryType, "cli")
	assertAttr(t, entry, attrAgentType, "my-agent")
	if !strings.Contains(attr(entry, attrInputMessages), "structured input") {
		t.Errorf("input = %s", attr(entry, attrInputMessages))
	}
}

func TestToolRetryEventsAreRecorded(t *testing.T) {
	env := newTestEnv(t, nil)
	h := env.handler
	info := &callbacks.RunInfo{Name: "a", Component: adk.ComponentOfAgent}
	ctx := h.OnStart(context.Background(), info, &adk.AgentCallbackInput{Input: &adk.AgentInput{Messages: []*schema.Message{schema.UserMessage("q")}}})
	iterator, generator := adk.NewAsyncIteratorPair[*adk.AgentEvent]()
	h.OnEnd(ctx, info, &adk.AgentCallbackOutput{Events: iterator})
	generator.Send(&adk.AgentEvent{Err: &adk.WillRetryError{RetryAttempt: 1}})
	generator.Send(adk.EventFromMessage(answerMessage("recovered", nil), nil, schema.Assistant, ""))
	generator.Close()
	agentSpan := env.spans().one(t, SpanKindAgent)
	assertStatus(t, agentSpan, codes.Ok)
	assertAttr(t, agentSpan, attrEinoRetryEventCount, "1")
	if !strings.Contains(attr(agentSpan, attrEinoRetryEvents), `"attempt":1`) {
		t.Errorf("retry events = %s", attr(agentSpan, attrEinoRetryEvents))
	}
	assertAttr(t, agentSpan, attrAgentMessageCount, "1")
	assertOutputText(t, agentSpan, "recovered")
}
