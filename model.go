// This file contains code adapted from github.com/cloudwego/eino-ext
// (callbacks/langfuse/v2), Copyright 2026 CloudWeGo Authors, licensed under
// the Apache License, Version 2.0. It was modified to emit Tencent Cloud CLS
// Agent Trace spans over OTLP/HTTP instead of Langfuse observations.

package agentobs

import (
	"fmt"

	"github.com/cloudwego/eino/callbacks"
	"github.com/cloudwego/eino/components/model"
	"github.com/cloudwego/eino/schema"
)

// chatRequest is the normalized ChatModel / AgenticModel input.
type chatRequest struct {
	model       string
	temperature *float32
	maxTokens   int
	topP        *float32
	stop        []string
	messages    []genAIMessage
	tools       []*schema.ToolInfo
}

// chatResponse is the normalized ChatModel / AgenticModel output.
type chatResponse struct {
	model    string
	messages []genAIMessage
	usage    *schema.TokenUsage
}

func extractChatRequest(agentic bool, inputs []callbacks.CallbackInput) (*chatRequest, error) {
	if agentic {
		return extractAgenticRequest(inputs)
	}
	request := &chatRequest{}
	var arrays [][]*schema.Message
	for _, input := range inputs {
		converted := model.ConvCallbackInput(input)
		if converted == nil {
			continue
		}
		if converted.Config != nil {
			config := converted.Config
			request.model = config.Model
			request.maxTokens = config.MaxTokens
			if config.Temperature != 0 {
				request.temperature = &config.Temperature
			}
			if config.TopP != 0 {
				request.topP = &config.TopP
			}
			request.stop = config.Stop
		}
		if len(converted.Tools) > 0 {
			request.tools = converted.Tools
		}
		if len(converted.Messages) > 0 {
			arrays = append(arrays, converted.Messages)
		}
	}
	messages, err := concatStreamedMessages(arrays)
	if err != nil {
		return request, err
	}
	request.messages = convertMessages(messages)
	return request, nil
}

func extractAgenticRequest(inputs []callbacks.CallbackInput) (*chatRequest, error) {
	request := &chatRequest{}
	var arrays [][]*schema.AgenticMessage
	for _, input := range inputs {
		converted := model.ConvAgenticCallbackInput(input)
		if converted == nil {
			continue
		}
		if converted.Config != nil {
			config := converted.Config
			request.model = config.Model
			request.maxTokens = config.MaxTokens
			if config.Temperature != 0 {
				request.temperature = &config.Temperature
			}
			if config.TopP != 0 {
				request.topP = &config.TopP
			}
		}
		if len(converted.Tools) > 0 {
			request.tools = converted.Tools
		}
		if len(converted.Messages) > 0 {
			arrays = append(arrays, converted.Messages)
		}
	}
	switch len(arrays) {
	case 0:
		return request, nil
	case 1:
		request.messages = convertAgenticMessages(arrays[0])
		return request, nil
	}
	expected := len(arrays[0])
	detached := make([][]*schema.AgenticMessage, 0, len(arrays))
	for _, messages := range arrays {
		if len(messages) != expected {
			return request, fmt.Errorf("unexpected streamed agentic message count: got %d, want %d", len(messages), expected)
		}
		copies := make([]*schema.AgenticMessage, len(messages))
		for i, message := range messages {
			copies[i] = detachAgenticMessage(message)
		}
		detached = append(detached, copies)
	}
	messages, err := schema.ConcatAgenticMessagesArray(detached)
	if err != nil {
		return request, fmt.Errorf("concat streamed agentic input: %w", err)
	}
	request.messages = convertAgenticMessages(messages)
	return request, nil
}

func concatStreamedMessages(arrays [][]*schema.Message) ([]*schema.Message, error) {
	switch len(arrays) {
	case 0:
		return nil, nil
	case 1:
		return arrays[0], nil
	}
	expected := len(arrays[0])
	detached := make([][]*schema.Message, 0, len(arrays))
	for _, messages := range arrays {
		if len(messages) != expected {
			return nil, fmt.Errorf("unexpected streamed message count: got %d, want %d", len(messages), expected)
		}
		copies := make([]*schema.Message, len(messages))
		for i, message := range messages {
			copies[i] = detachMessage(message)
		}
		detached = append(detached, copies)
	}
	return schema.ConcatMessageArray(detached)
}

// detachMessage copies the fields the SDK reads, leaving out Extra. Stream
// copies share chunk pointers, and ADK writes Extra (message IDs) on its own
// branch while callbacks read theirs; schema.ConcatMessages reads Extra, so
// concatenating the shared chunks directly would race.
func detachMessage(message *schema.Message) *schema.Message {
	if message == nil {
		return nil
	}
	return &schema.Message{
		Role:                     message.Role,
		Content:                  message.Content,
		MultiContent:             message.MultiContent,
		UserInputMultiContent:    message.UserInputMultiContent,
		AssistantGenMultiContent: message.AssistantGenMultiContent,
		Name:                     message.Name,
		ToolCalls:                message.ToolCalls,
		ToolCallID:               message.ToolCallID,
		ToolName:                 message.ToolName,
		ResponseMeta:             message.ResponseMeta,
		ReasoningContent:         message.ReasoningContent,
	}
}

func detachAgenticMessage(message *schema.AgenticMessage) *schema.AgenticMessage {
	if message == nil {
		return nil
	}
	return &schema.AgenticMessage{Role: message.Role, ContentBlocks: message.ContentBlocks, ResponseMeta: message.ResponseMeta}
}

// concatMessageChunks merges streamed chunks without touching their Extra.
func concatMessageChunks(chunks []*schema.Message) (*schema.Message, error) {
	detached := make([]*schema.Message, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk != nil {
			detached = append(detached, detachMessage(chunk))
		}
	}
	switch len(detached) {
	case 0:
		return nil, nil
	case 1:
		return detached[0], nil
	}
	return schema.ConcatMessages(detached)
}

func concatAgenticChunks(chunks []*schema.AgenticMessage) (*schema.AgenticMessage, error) {
	detached := make([]*schema.AgenticMessage, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk != nil {
			detached = append(detached, detachAgenticMessage(chunk))
		}
	}
	switch len(detached) {
	case 0:
		return nil, nil
	case 1:
		return detached[0], nil
	}
	return schema.ConcatAgenticMessages(detached)
}

func extractChatResponse(agentic bool, outputs []callbacks.CallbackOutput) (*chatResponse, error) {
	if agentic {
		return extractAgenticResponse(outputs)
	}
	response := &chatResponse{}
	var chunks []*schema.Message
	for _, output := range outputs {
		converted := model.ConvCallbackOutput(output)
		if converted == nil {
			continue
		}
		if converted.Message != nil {
			chunks = append(chunks, converted.Message)
		}
		if converted.Config != nil && converted.Config.Model != "" {
			response.model = converted.Config.Model
		}
		if converted.TokenUsage != nil {
			response.usage = modelUsageToSchema(converted.TokenUsage)
		}
	}
	if len(chunks) == 0 {
		return response, nil
	}
	message, err := concatMessageChunks(chunks)
	if err != nil {
		return response, fmt.Errorf("concat model output: %w", err)
	}
	if response.usage == nil && message.ResponseMeta != nil {
		response.usage = message.ResponseMeta.Usage
	}
	response.messages = convertMessages([]*schema.Message{message})
	return response, nil
}

func extractAgenticResponse(outputs []callbacks.CallbackOutput) (*chatResponse, error) {
	response := &chatResponse{}
	var chunks []*schema.AgenticMessage
	for _, output := range outputs {
		converted := model.ConvAgenticCallbackOutput(output)
		if converted == nil {
			continue
		}
		if converted.Message != nil {
			chunks = append(chunks, converted.Message)
		}
		if converted.Config != nil && converted.Config.Model != "" {
			response.model = converted.Config.Model
		}
		if converted.TokenUsage != nil {
			response.usage = modelUsageToSchema(converted.TokenUsage)
		}
	}
	if len(chunks) == 0 {
		return response, nil
	}
	message, err := concatAgenticChunks(chunks)
	if err != nil {
		return response, fmt.Errorf("concat agentic model output: %w", err)
	}
	if response.usage == nil && message.ResponseMeta != nil {
		response.usage = message.ResponseMeta.TokenUsage
	}
	response.messages = convertAgenticMessages([]*schema.AgenticMessage{message})
	return response, nil
}

func modelUsageToSchema(usage *model.TokenUsage) *schema.TokenUsage {
	return &schema.TokenUsage{
		PromptTokens:            usage.PromptTokens,
		PromptTokenDetails:      schema.PromptTokenDetails{CachedTokens: usage.PromptTokenDetails.CachedTokens},
		CompletionTokens:        usage.CompletionTokens,
		TotalTokens:             usage.TotalTokens,
		CompletionTokensDetails: schema.CompletionTokensDetails{ReasoningTokens: usage.CompletionTokensDetails.ReasoningTokens},
	}
}

// tokenTotals are normalized token counts, summed into agent spans.
type tokenTotals struct {
	input, output, total, cacheRead int
}

func (t *tokenTotals) add(other tokenTotals) {
	t.input += other.input
	t.output += other.output
	t.total += other.total
	t.cacheRead += other.cacheRead
}

// setUsage writes every CLS token dimension, including zeros, so dashboard
// SUM queries never return empty results. It returns the written values.
func setUsage(w *spanAttributeWriter, usage *schema.TokenUsage) tokenTotals {
	if usage == nil {
		return tokenTotals{}
	}
	input := max(usage.PromptTokens, 0)
	output := max(usage.CompletionTokens, 0)
	total := usage.TotalTokens
	if total <= 0 {
		total = input + output
	}
	cached := min(max(usage.PromptTokenDetails.CachedTokens, 0), input)
	reasoning := min(max(usage.CompletionTokensDetails.ReasoningTokens, 0), output)
	w.setInt(attrUsageInputTokens, input)
	w.setInt(attrUsageOutputTokens, output)
	w.setInt(attrUsageTotalTokens, total)
	w.setInt(attrUsageCacheReadTokens, cached)
	w.setInt(attrUsageCacheMissTokens, input-cached)
	w.setInt(attrUsageReasoningTokens, reasoning)
	return tokenTotals{input: input, output: output, total: total, cacheRead: cached}
}
