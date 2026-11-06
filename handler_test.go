package agentobs

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components"
	"github.com/cloudwego/eino/components/embedding"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"
	"go.opentelemetry.io/otel/codes"
)

type staticRetriever struct{}

func (staticRetriever) Retrieve(_ context.Context, query string, _ ...retriever.Option) ([]*schema.Document, error) {
	doc := (&schema.Document{ID: "doc-1", Content: "Eino is a Go LLM framework. query=" + query}).WithScore(0.87)
	return []*schema.Document{doc}, nil
}

func buildRAGChain(t *testing.T, chatModel model.BaseChatModel) compose.Runnable[string, *schema.Message] {
	t.Helper()
	chain := compose.NewChain[string, *schema.Message]().
		AppendRetriever(staticRetriever{}).
		AppendLambda(compose.InvokableLambda(func(_ context.Context, docs []*schema.Document) ([]*schema.Message, error) {
			return []*schema.Message{schema.SystemMessage(docs[0].Content), schema.UserMessage("what is eino?")}, nil
		})).
		AppendChatModel(chatModel)
	runnable, err := chain.Compile(context.Background(), compose.WithGraphName("rag"))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return runnable
}

func TestGraphAsAgent(t *testing.T) {
	env := newTestEnv(t, nil)
	runnable := buildRAGChain(t, &scriptedModel{responses: []*schema.Message{answerMessage("Eino is a framework.", nil)}})
	ctx := env.handler.StartTrace(context.Background())
	if _, err := runnable.Invoke(ctx, "eino", compose.WithCallbacks(env.handler)); err != nil {
		t.Fatal(err)
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()
	if len(spans) != 5 {
		t.Errorf("want entry/agent/retriever/step/chat, got %s", spans.describe())
	}

	entry := spans.one(t, SpanKindEntry)
	agent := spans.one(t, SpanKindAgent)
	ret := spans.one(t, SpanKindRetriever)
	step := spans.one(t, SpanKindStep)
	chat := spans.one(t, SpanKindChat)
	assertParent(t, agent, entry)
	assertParent(t, ret, agent)
	assertParent(t, step, agent)
	assertParent(t, chat, step)

	assertAttr(t, agent, attrEinoAgentSource, agentSourceGraph)
	assertAttr(t, agent, attrAgentName, "rag")
	assertAttr(t, agent, attrEinoComponent, string(compose.ComponentOfChain))
	assertOutputText(t, agent, "Eino is a framework.")
	assertOutputText(t, entry, "Eino is a framework.")
	if !strings.Contains(attr(entry, attrInputMessages), "eino") {
		t.Errorf("entry input = %s", attr(entry, attrInputMessages))
	}

	assertAttr(t, ret, attrOperationName, OperationRetrieval)
	assertAttr(t, ret, attrRetrievalQueryText, "eino")
	assertAttr(t, ret, attrRetrievalDocumentsCount, "1")
	assertAttr(t, ret, attrAgentName, "rag")
	var documents []retrievedDocument
	if err := json.Unmarshal([]byte(attr(ret, attrRetrievalDocuments)), &documents); err != nil || len(documents) != 1 {
		t.Fatalf("documents = %s (%v)", attr(ret, attrRetrievalDocuments), err)
	}
	if documents[0].ID != "doc-1" || documents[0].Score != 0.87 {
		t.Errorf("document = %+v", documents[0])
	}
	assertAttr(t, chat, attrReactRound, "1")
}

func TestRetrieverCountWithoutContentCapture(t *testing.T) {
	env := newTestEnv(t, func(c *Config) { c.DisableContentCapture = true })
	runnable := buildRAGChain(t, &scriptedModel{responses: []*schema.Message{answerMessage("ok", nil)}})
	if _, err := runnable.Invoke(context.Background(), "secret query", compose.WithCallbacks(env.handler)); err != nil {
		t.Fatal(err)
	}
	ret := env.spans().one(t, SpanKindRetriever)
	assertAttr(t, ret, attrRetrievalDocumentsCount, "1")
	for _, key := range []string{attrRetrievalQueryText, attrRetrievalDocuments} {
		if hasAttr(ret, key) {
			t.Errorf("content capture disabled but %s = %q", key, attr(ret, key))
		}
	}
}

func TestGraphWithoutAgentPromotion(t *testing.T) {
	env := newTestEnv(t, func(c *Config) { c.DisableGraphAsAgent = true })
	runnable := buildRAGChain(t, &scriptedModel{responses: []*schema.Message{answerMessage("ok", nil)}})
	ctx := env.handler.StartTrace(context.Background())
	if _, err := runnable.Invoke(ctx, "q", compose.WithCallbacks(env.handler)); err != nil {
		t.Fatal(err)
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()
	entry := spans.one(t, SpanKindEntry)
	if len(spans.byKind(SpanKindAgent)) != 0 || len(spans.byKind(SpanKindStep)) != 0 {
		t.Fatalf("unexpected agent/step spans: %s", spans.describe())
	}
	chat := spans.one(t, SpanKindChat)
	assertParent(t, chat, entry)
	assertParent(t, spans.one(t, SpanKindRetriever), entry)
	if hasAttr(chat, attrStepID) {
		t.Errorf("chat outside agents must not have a step id")
	}
	assertOutputText(t, entry, "ok")
}

func TestIncludeFrameworkSpans(t *testing.T) {
	env := newTestEnv(t, func(c *Config) { c.IncludeFrameworkSpans = true })
	runnable := buildRAGChain(t, &scriptedModel{responses: []*schema.Message{answerMessage("ok", nil)}})
	if _, err := runnable.Invoke(context.Background(), "q", compose.WithCallbacks(env.handler)); err != nil {
		t.Fatal(err)
	}
	spans := env.spans()
	agent := spans.one(t, SpanKindAgent)
	tasks := spans.byKind(SpanKindTask)
	if len(tasks) == 0 {
		t.Fatalf("framework spans missing: %s", spans.describe())
	}
	for _, task := range tasks {
		assertParent(t, task, agent)
		assertAttr(t, task, attrOperationName, OperationInvoke)
	}
}

func TestGraphErrorFailsEntry(t *testing.T) {
	env := newTestEnv(t, nil)
	runnable := buildRAGChain(t, &scriptedModel{errs: []error{errors.New("model down")}})
	ctx := env.handler.StartTrace(context.Background())
	if _, err := runnable.Invoke(ctx, "q", compose.WithCallbacks(env.handler)); err == nil {
		t.Fatal("expected error")
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()
	assertStatus(t, spans.one(t, SpanKindChat), codes.Error)
	assertStatus(t, spans.one(t, SpanKindAgent), codes.Error)
	entry := spans.one(t, SpanKindEntry)
	assertStatus(t, entry, codes.Error)
	if !strings.Contains(attr(entry, attrErrorMessage), "model down") {
		t.Errorf("entry error.message = %q", attr(entry, attrErrorMessage))
	}
}

func TestGraphStreamOutput(t *testing.T) {
	env := newTestEnv(t, nil)
	runnable := buildRAGChain(t, &scriptedModel{responses: []*schema.Message{answerMessage("streamed answer", &schema.TokenUsage{PromptTokens: 5, CompletionTokens: 2})}})
	ctx := env.handler.StartTrace(context.Background())
	stream, err := runnable.Stream(ctx, "q", compose.WithCallbacks(env.handler))
	if err != nil {
		t.Fatal(err)
	}
	var chunks []*schema.Message
	for {
		chunk, recvErr := stream.Recv()
		if recvErr != nil {
			break
		}
		chunks = append(chunks, chunk)
	}
	stream.Close()
	if len(chunks) != 2 {
		t.Fatalf("want 2 chunks, got %d", len(chunks))
	}
	env.handler.EndTrace(ctx, "")
	spans := env.spans()
	chat := spans.one(t, SpanKindChat)
	assertOutputText(t, chat, "streamed answer")
	assertAttr(t, chat, attrUsageTotalTokens, "7")
	if !hasAttr(chat, attrTimeToFirstTokenMs) {
		t.Errorf("ttft missing")
	}
	assertOutputText(t, spans.one(t, SpanKindAgent), "streamed answer")
	assertOutputText(t, spans.one(t, SpanKindEntry), "streamed answer")
}

func TestTypedModelCallbackInput(t *testing.T) {
	env := newTestEnv(t, nil)
	h := env.handler
	ctx := h.StartTrace(context.Background())
	info := &callbacks.RunInfo{Name: "ChatModel", Type: "OpenAI", Component: components.ComponentOfChatModel}
	temperature := float32(0.2)
	tools := []*schema.ToolInfo{{
		Name: "get_weather", Desc: "weather",
		ParamsOneOf: schema.NewParamsOneOfByParams(map[string]*schema.ParameterInfo{
			"city": {Type: schema.String, Required: true},
		}),
	}}
	ctx = h.OnStart(ctx, info, &model.CallbackInput{
		Messages: []*schema.Message{schema.UserMessage("hello")},
		Tools:    tools,
		Config:   &model.Config{Model: "gpt-4o", MaxTokens: 256, Temperature: temperature, Stop: []string{"END"}},
	})
	h.OnEnd(ctx, info, &model.CallbackOutput{
		Message:    answerMessage("hi", nil),
		Config:     &model.Config{Model: "gpt-4o-2024-08-06"},
		TokenUsage: &model.TokenUsage{PromptTokens: 9, CompletionTokens: 3, TotalTokens: 12},
	})
	h.EndTrace(ctx, "")
	chat := env.spans().one(t, SpanKindChat)
	if chat.Name != "chat gpt-4o" {
		t.Errorf("chat name = %q", chat.Name)
	}
	assertAttr(t, chat, attrProviderName, "openai")
	assertAttr(t, chat, attrRequestModel, "gpt-4o")
	assertAttr(t, chat, attrResponseModel, "gpt-4o-2024-08-06")
	assertAttr(t, chat, attrRequestMaxTokens, "256")
	assertAttr(t, chat, attrRequestStopSequences, `["END"]`)
	if value, ok := attrValue(chat, attrRequestTemperature); !ok || value.AsFloat64() < 0.19 || value.AsFloat64() > 0.21 {
		t.Errorf("temperature = %v", value.Emit())
	}
	assertAttr(t, chat, attrUsageCacheReadTokens, "0")
	assertAttr(t, chat, attrUsageCacheMissTokens, "9")
	definitions := attr(chat, attrToolDefinitions)
	if !strings.Contains(definitions, `"name":"get_weather"`) || !strings.Contains(definitions, `"parameters"`) {
		t.Errorf("tool definitions = %s", definitions)
	}
}

func TestEmbeddingCallback(t *testing.T) {
	env := newTestEnv(t, nil)
	h := env.handler
	info := &callbacks.RunInfo{Name: "Embedder", Type: "Ark", Component: components.ComponentOfEmbedding}
	ctx := h.OnStart(context.Background(), info, &embedding.CallbackInput{
		Texts: []string{"a", "b"}, Config: &embedding.Config{Model: "text-embedding-3"},
	})
	h.OnEnd(ctx, info, &embedding.CallbackOutput{
		Embeddings: [][]float64{{0.1, 0.2, 0.3}, {0.4, 0.5, 0.6}},
		TokenUsage: &embedding.TokenUsage{PromptTokens: 4, TotalTokens: 4},
	})
	span := env.spans().one(t, SpanKindEmbedding)
	if span.Name != "embeddings text-embedding-3" {
		t.Errorf("name = %q", span.Name)
	}
	assertAttr(t, span, attrEmbeddingsInputCount, "2")
	assertAttr(t, span, attrEmbeddingsDimensionCount, "3")
	assertAttr(t, span, attrUsageInputTokens, "4")
	assertAttr(t, span, attrRequestModel, "text-embedding-3")
	assertStatus(t, span, codes.Ok)
}

// Model fields are only written once the model responded, so a failed call
// (for example an unknown model) shows no model in CLS instead of the
// configured name. The span name still carries it for troubleshooting.
func TestFailedModelCallsHaveNoModelFields(t *testing.T) {
	env := newTestEnv(t, nil)
	h := env.handler
	ctx := h.StartTrace(context.Background())
	chatInfo := &callbacks.RunInfo{Name: "ChatModel", Type: "OpenAI", Component: components.ComponentOfChatModel}
	chatCtx := h.OnStart(ctx, chatInfo, &model.CallbackInput{
		Messages: []*schema.Message{schema.UserMessage("hello")},
		Config:   &model.Config{Model: "missing-model"},
	})
	h.OnError(chatCtx, chatInfo, errors.New("status code: 400, model does not exist"))
	embedInfo := &callbacks.RunInfo{Name: "Embedder", Type: "Ark", Component: components.ComponentOfEmbedding}
	embedCtx := h.OnStart(ctx, embedInfo, &embedding.CallbackInput{
		Texts: []string{"a"}, Config: &embedding.Config{Model: "missing-embedding"},
	})
	h.OnError(embedCtx, embedInfo, errors.New("status code: 400"))
	h.EndTrace(ctx, "")

	spans := env.spans()
	for kind, wantName := range map[string]string{
		SpanKindChat:      "chat missing-model",
		SpanKindEmbedding: "embeddings missing-embedding",
	} {
		span := spans.one(t, kind)
		assertStatus(t, span, codes.Error)
		if span.Name != wantName {
			t.Errorf("%s name = %q, want %q", kind, span.Name, wantName)
		}
		for _, key := range []string{attrRequestModel, attrResponseModel} {
			if hasAttr(span, key) {
				t.Errorf("%s must not carry %s=%q without a model response", kind, key, attr(span, key))
			}
		}
	}
	chat := spans.one(t, SpanKindChat)
	assertAttr(t, chat, attrFinishReasons, `["error"]`)
	assertAttr(t, chat, attrUsageInputTokens, "0")
	assertAttr(t, chat, attrUsageOutputTokens, "0")
	assertAttr(t, chat, attrUsageTotalTokens, "0")
	if hasAttr(chat, attrTimeToFirstTokenMs) {
		t.Errorf("failed chat must not carry time to first token")
	}
}

func TestNeededFiltersFrameworkComponents(t *testing.T) {
	env := newTestEnv(t, nil)
	h := env.handler
	for component, want := range map[components.Component]bool{
		components.ComponentOfChatModel: true,
		components.ComponentOfTool:      true,
		compose.ComponentOfGraph:        true,
		compose.ComponentOfLambda:       false,
		compose.ComponentOfToolsNode:    false,
		components.ComponentOfPrompt:    false,
	} {
		if got := h.Needed(context.Background(), &callbacks.RunInfo{Component: component}, callbacks.TimingOnStart); got != want {
			t.Errorf("Needed(%s) = %v, want %v", component, got, want)
		}
	}
}

func TestEndWithoutStartIsIgnored(t *testing.T) {
	env := newTestEnv(t, nil)
	info := &callbacks.RunInfo{Component: components.ComponentOfChatModel}
	env.handler.OnEnd(context.Background(), info, answerMessage("x", nil))
	env.handler.OnError(context.Background(), info, errors.New("x"))
	if spans := env.spans(); len(spans) != 0 {
		t.Errorf("unexpected spans: %s", spans.describe())
	}
}
