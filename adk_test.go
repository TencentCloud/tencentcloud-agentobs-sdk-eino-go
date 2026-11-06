package agentobs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func reactScript() *scriptedModel {
	return &scriptedModel{responses: []*schema.Message{
		toolCallMessage("call_1", "get_weather", `{"city":"Shenzhen"}`, &schema.TokenUsage{
			PromptTokens: 100, CompletionTokens: 20, TotalTokens: 120,
			PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 40},
		}),
		answerMessage("It is sunny in Shenzhen.", &schema.TokenUsage{
			PromptTokens: 150, CompletionTokens: 10, TotalTokens: 160,
			CompletionTokensDetails: schema.CompletionTokensDetails{ReasoningTokens: 4},
		}),
	}}
}

func runReactAgent(t *testing.T, env *testEnv, streaming bool, opts ...TraceOption) {
	t.Helper()
	agent := newTestAgent(t, "weather_agent", reactScript(), weatherTool(t, nil))
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: agent, EnableStreaming: streaming})
	ctx := env.handler.StartTrace(context.Background(), opts...)
	if err := drain(t, runner.Query(ctx, "What's the weather in Shenzhen?", adk.WithCallbacks(env.handler))); err != nil {
		t.Fatalf("agent run: %v", err)
	}
	env.handler.EndTrace(ctx, "")
}

func TestADKReactHierarchy(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		name := "generate"
		if streaming {
			name = "stream"
		}
		t.Run(name, func(t *testing.T) {
			env := newTestEnv(t, nil)
			runReactAgent(t, env, streaming,
				WithSessionID("sess-1"), WithTurnIndex(3), WithUserID("u-1"),
				WithAttributes(map[string]string{"git.repo": "demo", "gen_ai.span.kind": "spoofed"}))
			spans := env.spans()

			entry := spans.one(t, SpanKindEntry)
			agent := spans.one(t, SpanKindAgent)
			steps := spans.byKind(SpanKindStep)
			chats := spans.byKind(SpanKindChat)
			tool := spans.one(t, SpanKindTool)
			if len(steps) != 2 || len(chats) != 2 {
				t.Fatalf("want 2 steps and 2 chats, got %s", spans.describe())
			}
			if len(spans) != 7 {
				t.Errorf("unexpected extra spans: %s", spans.describe())
			}
			sortByStart(steps)
			sortByStart(chats)

			if entry.SpanKind != trace.SpanKindServer || entry.Parent.IsValid() {
				t.Errorf("entry must be a root SERVER span")
			}
			assertParent(t, agent, entry)
			assertParent(t, steps[0], agent)
			assertParent(t, steps[1], agent)
			assertParent(t, chats[0], steps[0])
			assertParent(t, tool, steps[0])
			assertParent(t, chats[1], steps[1])

			for _, span := range spans {
				assertAttr(t, span, attrSessionID, "sess-1")
				assertAttr(t, span, attrTurnID, "sess-1:t3")
				assertAttr(t, span, attrUserID, "u-1")
				assertAttr(t, span, attrAgentType, defaultAgentType)
				assertAttr(t, span, "git.repo", "demo")
				assertStatus(t, span, codes.Ok)
			}

			assertAttr(t, entry, attrOperationName, OperationEnterApplication)
			assertAttr(t, entry, attrEntryType, defaultEntryType)
			if !strings.Contains(attr(entry, attrInputMessages), "What's the weather in Shenzhen?") {
				t.Errorf("entry input = %s", attr(entry, attrInputMessages))
			}
			if !hasAttr(entry, attrInputMessagesHash) {
				t.Errorf("entry input hash missing")
			}
			assertOutputText(t, entry, "It is sunny in Shenzhen.")

			assertAttr(t, agent, attrOperationName, OperationInvokeAgent)
			assertAttr(t, agent, attrAgentName, "weather_agent")
			assertAttr(t, agent, attrAgentID, agent.SpanContext.SpanID().String())
			assertAttr(t, agent, attrEinoAgentSource, agentSourceADK)
			assertAttr(t, agent, attrAgentToolCallCount, "1")
			assertAttr(t, agent, attrUsageInputTokens, "250")
			assertAttr(t, agent, attrUsageOutputTokens, "30")
			assertAttr(t, agent, attrUsageTotalTokens, "280")
			assertAttr(t, agent, attrUsageCacheReadTokens, "40")
			if hasAttr(agent, attrAgentScope) || hasAttr(agent, attrSubagentParentToolCallID) {
				t.Errorf("top-level agent must not be marked as subagent")
			}
			if agent.Name != "invoke_agent weather_agent" {
				t.Errorf("agent span name = %q", agent.Name)
			}
			assertOutputText(t, agent, "It is sunny in Shenzhen.")

			for index, step := range steps {
				round := index + 1
				assertAttr(t, step, attrOperationName, OperationReact)
				assertAttr(t, step, attrReactRound, itoa(round))
				assertAttr(t, step, attrStepID, "sess-1:t3:s"+itoa(round))
				assertAttr(t, step, attrAgentID, attr(agent, attrAgentID))
				assertAttr(t, chats[index], attrStepID, attr(step, attrStepID))
				if step.Name != "react round_"+itoa(round) {
					t.Errorf("step name = %q", step.Name)
				}
			}
			assertAttr(t, steps[0], attrReactFinishReason, "tool_calls")
			assertAttr(t, steps[1], attrReactFinishReason, "stop")
			assertAttr(t, tool, attrStepID, attr(steps[0], attrStepID))

			first := chats[0]
			assertAttr(t, first, attrOperationName, OperationChat)
			assertAttr(t, first, attrAgentName, "weather_agent")
			assertAttr(t, first, attrUsageInputTokens, "100")
			assertAttr(t, first, attrUsageOutputTokens, "20")
			assertAttr(t, first, attrUsageTotalTokens, "120")
			assertAttr(t, first, attrUsageCacheReadTokens, "40")
			assertAttr(t, first, attrUsageCacheMissTokens, "60")
			assertAttr(t, first, attrUsageReasoningTokens, "0")
			assertAttr(t, first, attrReactFinishReason, "tool_calls")
			assertAttr(t, first, attrFinishReasons, `["tool_calls"]`)
			if !hasAttr(first, attrChatDurationMs) || !hasAttr(first, attrInputMessagesHash) {
				t.Errorf("chat duration/hash missing")
			}
			var output []genAIMessage
			if err := json.Unmarshal([]byte(attr(first, attrOutputMessages)), &output); err != nil {
				t.Fatalf("chat output is not JSON: %v", err)
			}
			if len(output) != 1 || len(output[0].Parts) != 1 || output[0].Parts[0].Type != "tool_call" || output[0].Parts[0].ID != "call_1" {
				t.Errorf("chat output = %+v", output)
			}
			assertAttr(t, chats[1], attrUsageReasoningTokens, "4")
			if !strings.Contains(attr(chats[1], attrInputMessages), `"type":"tool_call_response"`) {
				t.Errorf("second chat input lacks tool response: %s", attr(chats[1], attrInputMessages))
			}
			// Non-streaming calls deliver the first token with the response.
			for _, chat := range chats {
				ttft, ok := attrValue(chat, attrTimeToFirstTokenMs)
				duration, _ := attrValue(chat, attrChatDurationMs)
				if !ok || ttft.AsInt64() < 0 || ttft.AsInt64() > duration.AsInt64() {
					t.Errorf("time to first token = %v (present %v), duration %v", ttft.Emit(), ok, duration.Emit())
				}
			}

			assertAttr(t, tool, attrOperationName, OperationExecuteTool)
			assertAttr(t, tool, attrToolName, "get_weather")
			assertAttr(t, tool, attrToolCallID, "call_1")
			assertAttr(t, tool, attrToolType, toolTypeFunction)
			assertAttr(t, tool, attrToolCallArguments, `{"city":"Shenzhen"}`)
			if !strings.Contains(attr(tool, attrToolCallResult), "sunny") {
				t.Errorf("tool result = %s", attr(tool, attrToolCallResult))
			}
			if tool.Name != "execute_tool get_weather" || !hasAttr(tool, attrToolCallDurationMs) {
				t.Errorf("tool span name/duration wrong: %q", tool.Name)
			}
		})
	}
}

func TestADKWithoutStartTraceStillLinksSpans(t *testing.T) {
	env := newTestEnv(t, nil)
	agent := newTestAgent(t, "weather_agent", reactScript(), weatherTool(t, nil))
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: agent})
	if err := drain(t, runner.Query(context.Background(), "hi", adk.WithCallbacks(env.handler))); err != nil {
		t.Fatal(err)
	}
	spans := env.spans()
	if len(spans.byKind(SpanKindEntry)) != 0 {
		t.Fatalf("no entry span expected without StartTrace")
	}
	agentSpan := spans.one(t, SpanKindAgent)
	if agentSpan.Parent.IsValid() {
		t.Errorf("agent should be the root span")
	}
	traceHex := agentSpan.SpanContext.TraceID().String()
	for _, span := range spans {
		if span.SpanContext.TraceID() != agentSpan.SpanContext.TraceID() {
			t.Errorf("%s in another trace", span.Name)
		}
		assertAttr(t, span, attrSessionID, traceHex)
		assertAttr(t, span, attrTurnID, traceHex)
	}
}

func TestADKToolErrorFailsTrace(t *testing.T) {
	env := newTestEnv(t, nil)
	failing := weatherTool(t, func(context.Context, weatherInput) (string, error) {
		return "", errors.New("weather backend unavailable")
	})
	agent := newTestAgent(t, "weather_agent", reactScript(), failing)
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: agent})
	ctx := env.handler.StartTrace(context.Background())
	if err := drain(t, runner.Query(ctx, "weather?", adk.WithCallbacks(env.handler))); err == nil {
		t.Fatalf("expected agent error")
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()

	tool := spans.one(t, SpanKindTool)
	assertStatus(t, tool, codes.Error)
	assertAttr(t, tool, attrErrorType, "tool_error")
	assertAttr(t, tool, attrToolErrorType, "tool_error")
	if !strings.Contains(attr(tool, attrErrorMessage), "weather backend unavailable") {
		t.Errorf("tool error message = %q", attr(tool, attrErrorMessage))
	}
	agentSpan := spans.one(t, SpanKindAgent)
	assertStatus(t, agentSpan, codes.Error)
	assertAttr(t, agentSpan, attrErrorType, "agent_error")
	entry := spans.one(t, SpanKindEntry)
	assertStatus(t, entry, codes.Error)
	if attr(entry, attrErrorType) == "" {
		t.Errorf("entry error.type missing")
	}
}

func TestADKModelErrorMarksStep(t *testing.T) {
	env := newTestEnv(t, nil)
	chatModel := &scriptedModel{errs: []error{errors.New("rate limited")}}
	agent := newTestAgent(t, "a", chatModel)
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: agent})
	ctx := env.handler.StartTrace(context.Background())
	_ = drain(t, runner.Query(ctx, "hi", adk.WithCallbacks(env.handler)))
	env.handler.EndTrace(ctx, "")
	spans := env.spans()

	chat := spans.one(t, SpanKindChat)
	assertStatus(t, chat, codes.Error)
	assertAttr(t, chat, attrErrorType, "chat_error")
	assertAttr(t, chat, attrFinishReasons, `["error"]`)
	assertAttr(t, chat, attrUsageInputTokens, "0")
	assertAttr(t, chat, attrUsageOutputTokens, "0")
	assertAttr(t, chat, attrReactFinishReason, "error")
	step := spans.one(t, SpanKindStep)
	assertStatus(t, step, codes.Error)
	assertAttr(t, step, attrReactFinishReason, "error")
	assertParent(t, chat, step)
	agentSpan := spans.one(t, SpanKindAgent)
	assertAttr(t, agentSpan, attrUsageInputTokens, "0")
	assertAttr(t, agentSpan, attrUsageTotalTokens, "0")
	assertStatus(t, spans.one(t, SpanKindEntry), codes.Error)
}

func TestADKCancellationIsSoft(t *testing.T) {
	env := newTestEnv(t, nil)
	chatModel := &scriptedModel{errs: []error{context.Canceled}}
	agent := newTestAgent(t, "a", chatModel)
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: agent})
	ctx := env.handler.StartTrace(context.Background())
	_ = drain(t, runner.Query(ctx, "hi", adk.WithCallbacks(env.handler)))
	env.handler.EndTrace(ctx, "")
	spans := env.spans()

	for _, kind := range []string{SpanKindEntry, SpanKindAgent, SpanKindChat} {
		span := spans.one(t, kind)
		assertStatus(t, span, codes.Unset)
		assertAttr(t, span, attrEinoStatus, statusCancelled)
		if hasAttr(span, attrErrorType) {
			t.Errorf("%s must not carry error.type", kind)
		}
	}
	chat := spans.one(t, SpanKindChat)
	assertAttr(t, chat, attrFinishReasons, `["cancelled"]`)
	assertAttr(t, chat, attrUsageInputTokens, "0")
	assertAttr(t, chat, attrUsageOutputTokens, "0")
	if reason := attr(spans.one(t, SpanKindStep), attrReactFinishReason); reason == "error" {
		t.Errorf("cancelled step must not report react.finish_reason=error")
	}
}

func TestNestedAgentToolKeepsOwnSteps(t *testing.T) {
	env := newTestEnv(t, nil)
	inner := newTestAgent(t, "inner", &scriptedModel{responses: []*schema.Message{
		answerMessage("inner answer", &schema.TokenUsage{PromptTokens: 10, CompletionTokens: 2, TotalTokens: 12}),
	}})
	innerTool := adk.NewAgentTool(context.Background(), inner)
	outerModel := &scriptedModel{responses: []*schema.Message{
		toolCallMessage("call_inner", "inner", `{"request":"do it"}`, &schema.TokenUsage{
			PromptTokens: 100, CompletionTokens: 20, TotalTokens: 120,
			PromptTokenDetails: schema.PromptTokenDetails{CachedTokens: 40},
		}),
		answerMessage("outer answer", &schema.TokenUsage{PromptTokens: 150, CompletionTokens: 10, TotalTokens: 160}),
	}}
	outer := newTestAgent(t, "outer", outerModel, innerTool)
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: outer})
	ctx := env.handler.StartTrace(context.Background())
	if err := drain(t, runner.Query(ctx, "go", adk.WithCallbacks(env.handler))); err != nil {
		t.Fatal(err)
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()

	agents := spans.byKind(SpanKindAgent)
	if len(agents) != 2 {
		t.Fatalf("want 2 agents, got %s", spans.describe())
	}
	byName := map[string]tracetest.SpanStub{}
	for _, span := range agents {
		byName[attr(span, attrAgentName)] = span
	}
	outerSpan, innerSpan := byName["outer"], byName["inner"]
	tool := spans.one(t, SpanKindTool)
	assertParent(t, innerSpan, tool)
	assertAttr(t, outerSpan, attrAgentToolCallCount, "1")
	assertAttr(t, innerSpan, attrAgentToolCallCount, "0")

	assertAttr(t, outerSpan, attrOperationName, OperationInvokeAgent)
	assertAttr(t, innerSpan, attrOperationName, OperationInvokeSubagent)
	assertAttr(t, innerSpan, attrAgentScope, agentScopeSubagent)
	assertAttr(t, innerSpan, attrSubagentParentToolCallID, attr(tool, attrToolCallID))
	assertAttr(t, innerSpan, attrSubagentParentToolCallID, "call_inner")
	if hasAttr(outerSpan, attrAgentScope) {
		t.Errorf("outer agent must not be marked as subagent")
	}
	// Agent token totals cover the agent's whole subtree.
	assertAttr(t, innerSpan, attrUsageInputTokens, "10")
	assertAttr(t, innerSpan, attrUsageOutputTokens, "2")
	assertAttr(t, innerSpan, attrUsageTotalTokens, "12")
	assertAttr(t, innerSpan, attrUsageCacheReadTokens, "0")
	assertAttr(t, outerSpan, attrUsageInputTokens, "260")
	assertAttr(t, outerSpan, attrUsageOutputTokens, "32")
	assertAttr(t, outerSpan, attrUsageTotalTokens, "292")
	assertAttr(t, outerSpan, attrUsageCacheReadTokens, "40")

	stepsByAgent := map[string]int{}
	for _, step := range spans.byKind(SpanKindStep) {
		stepsByAgent[attr(step, attrAgentName)]++
	}
	if stepsByAgent["outer"] != 2 || stepsByAgent["inner"] != 1 {
		t.Errorf("steps per agent = %v", stepsByAgent)
	}
	assertOutputText(t, spans.one(t, SpanKindEntry), "outer answer")
}

func TestExplicitEndTraceOutputWins(t *testing.T) {
	env := newTestEnv(t, nil)
	agent := newTestAgent(t, "a", &scriptedModel{responses: []*schema.Message{answerMessage("model answer", nil)}})
	runner := adk.NewRunner(context.Background(), adk.RunnerConfig{Agent: agent})
	ctx := env.handler.StartTrace(context.Background(), WithInput("original question"), WithName("my_app"))
	_ = drain(t, runner.Query(ctx, "rewritten question", adk.WithCallbacks(env.handler)))
	env.handler.EndTrace(ctx, "final answer")
	env.handler.EndTrace(ctx, "ignored second call")
	entry := env.spans().one(t, SpanKindEntry)
	if entry.Name != "my_app" {
		t.Errorf("entry name = %q", entry.Name)
	}
	assertOutputText(t, entry, "final answer")
	if input := attr(entry, attrInputMessages); !strings.Contains(input, "original question") || strings.Contains(input, "rewritten") {
		t.Errorf("entry input = %s", input)
	}
}

func TestDisableContentCapture(t *testing.T) {
	env := newTestEnv(t, func(c *Config) { c.DisableContentCapture = true })
	runReactAgent(t, env, false)
	for _, span := range env.spans() {
		for _, key := range []string{attrInputMessages, attrOutputMessages, attrToolCallArguments, attrToolCallResult} {
			if hasAttr(span, key) {
				t.Errorf("%s leaked %s", span.Name, key)
			}
		}
	}
}

func TestMaskFuncAppliesToContent(t *testing.T) {
	env := newTestEnv(t, func(c *Config) {
		c.MaskFunc = func(value string) string { return strings.ReplaceAll(value, "Shenzhen", "***") }
	})
	runReactAgent(t, env, false)
	for _, span := range env.spans() {
		for _, kv := range span.Attributes {
			if strings.Contains(kv.Value.Emit(), "Shenzhen") {
				t.Errorf("%s %s not masked: %s", span.Name, kv.Key, kv.Value.Emit())
			}
		}
	}
}

func TestDisableStepSpansKeepsIdentifiers(t *testing.T) {
	env := newTestEnv(t, func(c *Config) { c.DisableStepSpans = true })
	runReactAgent(t, env, false)
	spans := env.spans()
	if len(spans.byKind(SpanKindStep)) != 0 {
		t.Fatalf("step spans emitted: %s", spans.describe())
	}
	agentSpan := spans.one(t, SpanKindAgent)
	for _, chat := range spans.byKind(SpanKindChat) {
		assertParent(t, chat, agentSpan)
		if attr(chat, attrStepID) == "" || attr(chat, attrReactRound) == "" {
			t.Errorf("chat lacks step identifiers")
		}
	}
	assertParent(t, spans.one(t, SpanKindTool), agentSpan)
}

func TestWithTraceIDHonoured(t *testing.T) {
	env := newTestEnv(t, nil)
	const traceHex = "0af7651916cd43dd8448eb211c80319c"
	ctx := env.handler.StartTrace(context.Background(), WithTraceID(traceHex))
	env.handler.EndTrace(ctx, "")
	entry := env.spans().one(t, SpanKindEntry)
	if entry.SpanContext.TraceID().String() != traceHex {
		t.Errorf("trace id = %s", entry.SpanContext.TraceID())
	}
	assertAttr(t, entry, attrSessionID, traceHex)
}

func TestEntryEndsWhenContextCancelled(t *testing.T) {
	env := newTestEnv(t, nil)
	parent, cancel := context.WithCancel(context.Background())
	env.handler.StartTrace(parent)
	cancel()
	deadline := make(chan struct{})
	go func() {
		defer close(deadline)
		for len(env.exporter.GetSpans()) == 0 {
			_ = env.handler.provider.ForceFlush(context.Background())
		}
	}()
	<-deadline
	entry := env.spans().one(t, SpanKindEntry)
	assertAttr(t, entry, attrEinoStatus, statusCancelled)
	assertStatus(t, entry, codes.Unset)
}

// A request context that ends after EndTrace (for example an HTTP handler
// returning while a child callback is still finishing) must not turn a
// completed trace into a cancelled or failed one.
func TestContextEndAfterEndTraceKeepsOutcome(t *testing.T) {
	for name, end := range map[string]func(context.Context) (context.Context, func()){
		"canceled": func(ctx context.Context) (context.Context, func()) {
			ctx, cancel := context.WithCancel(ctx)
			return ctx, cancel
		},
		"deadline": func(ctx context.Context) (context.Context, func()) {
			ctx, cancel := context.WithDeadline(ctx, time.Now().Add(20*time.Millisecond))
			return ctx, func() { <-ctx.Done(); cancel() }
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := newTestEnv(t, nil)
			parent, finish := end(context.Background())
			traceCtx := env.handler.StartTrace(parent)
			info := &callbacks.RunInfo{Name: "late_model", Component: components.ComponentOfChatModel}
			childCtx := env.handler.OnStart(traceCtx, info, &model.CallbackInput{Messages: []*schema.Message{schema.UserMessage("hi")}})

			env.handler.EndTrace(traceCtx, "done")
			finish()
			<-parent.Done()
			time.Sleep(20 * time.Millisecond)
			env.handler.OnEnd(childCtx, info, &model.CallbackOutput{Message: schema.AssistantMessage("done", nil)})

			entry := env.spans().one(t, SpanKindEntry)
			if hasAttr(entry, attrEinoStatus) {
				t.Errorf("entry eino.status = %q, want unset", attr(entry, attrEinoStatus))
			}
			assertStatus(t, entry, codes.Ok)
			assertOutputText(t, entry, "done")
		})
	}
}

func assertOutputText(t *testing.T, span tracetest.SpanStub, want string) {
	t.Helper()
	var messages []genAIMessage
	raw := attr(span, attrOutputMessages)
	if err := json.Unmarshal([]byte(raw), &messages); err != nil {
		t.Fatalf("%s output is not valid JSON: %v (%s)", span.Name, err, raw)
	}
	if len(messages) == 0 {
		t.Fatalf("%s has no output messages", span.Name)
	}
	last := messages[len(messages)-1]
	if last.Role != "assistant" || len(last.Parts) == 0 || last.Parts[0].Content != want {
		t.Errorf("%s output = %s, want %q", span.Name, raw, want)
	}
}

func sortByStart(spans []tracetest.SpanStub) {
	for i := 1; i < len(spans); i++ {
		for j := i; j > 0 && spans[j].StartTime.Before(spans[j-1].StartTime); j-- {
			spans[j], spans[j-1] = spans[j-1], spans[j]
		}
	}
}

func itoa(value int) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
