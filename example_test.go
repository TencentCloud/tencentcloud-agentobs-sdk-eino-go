package agentobs_test

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/cloudwego/eino/adk"
	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/compose"
	"github.com/cloudwego/eino/schema"

	agentobs "git.woa.com/albertbi/tencentcloud-agentobs-sdk-eino-go"
)

// chatModel stands for any Eino ToolCallingChatModel, for example one created
// with github.com/cloudwego/eino-ext/components/model/openai.
var chatModel model.ToolCallingChatModel

// Credentials, region and topic come from TENCENTCLOUD_SECRET_ID,
// TENCENTCLOUD_SECRET_KEY, CLS_DEFAULT_REGION and CLS_TOPIC_ID.
func ExampleNewHandler() {
	ctx := context.Background()
	handler, err := agentobs.NewHandler(ctx, &agentobs.Config{
		ServiceName: "customer-support-agent",
		Environment: "production",
		Release:     "v1.4.0",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = handler.Shutdown(shutdownCtx)
	}()

	agent, err := adk.NewChatModelAgent(ctx, &adk.ChatModelAgentConfig{
		Name:        "support",
		Description: "answers customer questions",
		Model:       chatModel,
	})
	if err != nil {
		log.Fatal(err)
	}
	runner := adk.NewRunner(ctx, adk.RunnerConfig{Agent: agent, EnableStreaming: true})

	// One StartTrace per user request (turn).
	traceCtx := handler.StartTrace(ctx,
		agentobs.WithSessionID("conversation-42"),
		agentobs.WithTurnIndex(3),
		agentobs.WithUserID("user-1001"),
		agentobs.WithEntryType("api"),
	)
	events := runner.Query(traceCtx, "Where is my order?", adk.WithCallbacks(handler))
	for {
		event, ok := events.Next()
		if !ok {
			break
		}
		if event.Err != nil {
			log.Printf("agent error: %v", event.Err)
			continue
		}
		if output := event.Output; output != nil && output.MessageOutput != nil {
			if _, err := output.MessageOutput.GetMessage(); err != nil {
				log.Printf("read message: %v", err)
			}
		}
	}
	// The final answer defaults to the agent output; pass a string to override.
	handler.EndTrace(traceCtx, "")
}

// Graphs, chains and workflows: the outermost graph becomes the agent span.
func ExampleHandler_StartTrace_graph() {
	ctx := context.Background()
	handler, err := agentobs.NewHandler(ctx, &agentobs.Config{
		Region:  "ap-guangzhou",
		TopicID: "your-trace-topic-id",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = handler.Shutdown(context.Background()) }()

	chain, err := compose.NewChain[[]*schema.Message, *schema.Message]().
		AppendChatModel(chatModel).
		Compile(ctx, compose.WithGraphName("qa"))
	if err != nil {
		log.Fatal(err)
	}
	traceCtx := handler.StartTrace(ctx, agentobs.WithUserID("user-1001"))
	defer handler.EndTrace(traceCtx, "")
	if _, err := chain.Invoke(traceCtx, []*schema.Message{schema.UserMessage("hi")}, compose.WithCallbacks(handler)); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("invoke: %v", err)
	}
}

// Register globally to trace every Eino component without passing options.
func ExampleHandler_global() {
	handler, err := agentobs.NewHandler(context.Background(), &agentobs.Config{})
	if err != nil {
		log.Fatal(err)
	}
	callbacks.AppendGlobalHandlers(handler)
}
