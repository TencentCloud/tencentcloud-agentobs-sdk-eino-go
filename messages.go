package agentobs

import (
	"encoding/json"
	"strings"

	"github.com/cloudwego/eino/schema"
)

// genAIMessage follows the OpenTelemetry GenAI input/output message schema
// consumed by CLS: {"role": ..., "parts": [...], "finish_reason": ...}.
type genAIMessage struct {
	Role         string      `json:"role"`
	Parts        []genAIPart `json:"parts"`
	Name         string      `json:"name,omitempty"`
	FinishReason string      `json:"finish_reason,omitempty"`
}

type genAIPart struct {
	Type      string `json:"type"`
	Content   any    `json:"content,omitempty"`
	ID        string `json:"id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments any    `json:"arguments,omitempty"`
	Response  any    `json:"response,omitempty"`
	Modality  string `json:"modality,omitempty"`
	MIMEType  string `json:"mime_type,omitempty"`
	URI       string `json:"uri,omitempty"`
}

type genAIToolDefinition struct {
	Type        string `json:"type"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

func textMessage(role, text string) genAIMessage {
	return genAIMessage{Role: role, Parts: []genAIPart{{Type: "text", Content: text}}}
}

func convertMessages(messages []*schema.Message) []genAIMessage {
	result := make([]genAIMessage, 0, len(messages))
	for _, message := range messages {
		if converted, ok := convertMessage(message); ok {
			result = append(result, converted)
		}
	}
	return result
}

func convertMessage(message *schema.Message) (genAIMessage, bool) {
	if message == nil {
		return genAIMessage{}, false
	}
	result := genAIMessage{Role: string(message.Role), Parts: []genAIPart{}}
	if message.Role == schema.Tool {
		result.Name = message.ToolName
		result.Parts = append(result.Parts, genAIPart{
			Type:     "tool_call_response",
			ID:       message.ToolCallID,
			Response: toolMessageResponse(message),
		})
		return result, true
	}
	result.Name = message.Name
	if message.ReasoningContent != "" {
		result.Parts = append(result.Parts, genAIPart{Type: "reasoning", Content: message.ReasoningContent})
	}
	if message.Content != "" {
		result.Parts = append(result.Parts, genAIPart{Type: "text", Content: message.Content})
	}
	switch {
	case len(message.UserInputMultiContent) > 0:
		for _, part := range message.UserInputMultiContent {
			result.Parts = appendPart(result.Parts, convertInputPart(part))
		}
	case len(message.AssistantGenMultiContent) > 0:
		for _, part := range message.AssistantGenMultiContent {
			result.Parts = appendPart(result.Parts, convertOutputPart(part, message.ReasoningContent != ""))
		}
	default:
		for _, part := range message.MultiContent {
			result.Parts = appendPart(result.Parts, convertLegacyPart(part))
		}
	}
	for _, call := range message.ToolCalls {
		result.Parts = append(result.Parts, genAIPart{
			Type:      "tool_call",
			ID:        call.ID,
			Name:      call.Function.Name,
			Arguments: jsonOrString(call.Function.Arguments),
		})
	}
	if message.ResponseMeta != nil {
		result.FinishReason = message.ResponseMeta.FinishReason
	}
	return result, true
}

func toolMessageResponse(message *schema.Message) any {
	if message.Content != "" || len(message.UserInputMultiContent) == 0 {
		return jsonOrString(message.Content)
	}
	parts := make([]genAIPart, 0, len(message.UserInputMultiContent))
	for _, part := range message.UserInputMultiContent {
		parts = appendPart(parts, convertInputPart(part))
	}
	return parts
}

func appendPart(parts []genAIPart, part *genAIPart) []genAIPart {
	if part == nil {
		return parts
	}
	return append(parts, *part)
}

func convertInputPart(part schema.MessageInputPart) *genAIPart {
	switch part.Type {
	case schema.ChatMessagePartTypeText:
		if part.Text == "" {
			return nil
		}
		return &genAIPart{Type: "text", Content: part.Text}
	case schema.ChatMessagePartTypeImageURL:
		if part.Image != nil {
			return mediaPart("image", &part.Image.MessagePartCommon, "")
		}
	case schema.ChatMessagePartTypeAudioURL:
		if part.Audio != nil {
			return mediaPart("audio", &part.Audio.MessagePartCommon, "")
		}
	case schema.ChatMessagePartTypeVideoURL:
		if part.Video != nil {
			return mediaPart("video", &part.Video.MessagePartCommon, "")
		}
	case schema.ChatMessagePartTypeFileURL:
		if part.File != nil {
			return mediaPart("file", &part.File.MessagePartCommon, part.File.Name)
		}
	}
	return nil
}

func convertOutputPart(part schema.MessageOutputPart, hasReasoning bool) *genAIPart {
	switch part.Type {
	case schema.ChatMessagePartTypeText:
		if part.Text == "" {
			return nil
		}
		return &genAIPart{Type: "text", Content: part.Text}
	case schema.ChatMessagePartTypeReasoning:
		if hasReasoning || part.Reasoning == nil || part.Reasoning.Text == "" {
			return nil
		}
		return &genAIPart{Type: "reasoning", Content: part.Reasoning.Text}
	case schema.ChatMessagePartTypeImageURL:
		if part.Image != nil {
			return mediaPart("image", &part.Image.MessagePartCommon, "")
		}
	case schema.ChatMessagePartTypeAudioURL:
		if part.Audio != nil {
			return mediaPart("audio", &part.Audio.MessagePartCommon, "")
		}
	case schema.ChatMessagePartTypeVideoURL:
		if part.Video != nil {
			return mediaPart("video", &part.Video.MessagePartCommon, "")
		}
	}
	return nil
}

func convertLegacyPart(part schema.ChatMessagePart) *genAIPart {
	switch part.Type {
	case schema.ChatMessagePartTypeText:
		if part.Text == "" {
			return nil
		}
		return &genAIPart{Type: "text", Content: part.Text}
	case schema.ChatMessagePartTypeImageURL:
		if part.ImageURL != nil {
			return uriPart("image", firstNonEmpty(part.ImageURL.URL, part.ImageURL.URI), part.ImageURL.MIMEType, "")
		}
	case schema.ChatMessagePartTypeAudioURL:
		if part.AudioURL != nil {
			return uriPart("audio", firstNonEmpty(part.AudioURL.URL, part.AudioURL.URI), part.AudioURL.MIMEType, "")
		}
	case schema.ChatMessagePartTypeVideoURL:
		if part.VideoURL != nil {
			return uriPart("video", firstNonEmpty(part.VideoURL.URL, part.VideoURL.URI), part.VideoURL.MIMEType, "")
		}
	case schema.ChatMessagePartTypeFileURL:
		if part.FileURL != nil {
			return uriPart("file", firstNonEmpty(part.FileURL.URL, part.FileURL.URI), part.FileURL.MIMEType, part.FileURL.Name)
		}
	}
	return nil
}

// mediaPart never copies inline base64 payloads; only the modality and MIME
// type are recorded for blobs.
func mediaPart(modality string, common *schema.MessagePartCommon, name string) *genAIPart {
	if common.URL != nil && *common.URL != "" {
		return uriPart(modality, *common.URL, common.MIMEType, name)
	}
	return &genAIPart{Type: "blob", Modality: modality, MIMEType: common.MIMEType, Name: name}
}

func uriPart(modality, uri, mimeType, name string) *genAIPart {
	if uri == "" || strings.HasPrefix(uri, "data:") {
		return &genAIPart{Type: "blob", Modality: modality, MIMEType: mimeType, Name: name}
	}
	return &genAIPart{Type: "uri", Modality: modality, URI: uri, MIMEType: mimeType, Name: name}
}

func convertAgenticMessages(messages []*schema.AgenticMessage) []genAIMessage {
	result := make([]genAIMessage, 0, len(messages))
	for _, message := range messages {
		if converted, ok := convertAgenticMessage(message); ok {
			result = append(result, converted)
		}
	}
	return result
}

func convertAgenticMessage(message *schema.AgenticMessage) (genAIMessage, bool) {
	if message == nil {
		return genAIMessage{}, false
	}
	result := genAIMessage{Role: string(message.Role), Parts: []genAIPart{}}
	hasToolCall := false
	for _, block := range message.ContentBlocks {
		if block == nil {
			continue
		}
		switch {
		case block.Reasoning != nil:
			if block.Reasoning.Text != "" {
				result.Parts = append(result.Parts, genAIPart{Type: "reasoning", Content: block.Reasoning.Text})
			}
		case block.UserInputText != nil:
			result.Parts = append(result.Parts, genAIPart{Type: "text", Content: block.UserInputText.Text})
		case block.AssistantGenText != nil:
			result.Parts = append(result.Parts, genAIPart{Type: "text", Content: block.AssistantGenText.Text})
		case block.FunctionToolCall != nil:
			hasToolCall = true
			result.Parts = append(result.Parts, genAIPart{
				Type:      "tool_call",
				ID:        block.FunctionToolCall.CallID,
				Name:      block.FunctionToolCall.Name,
				Arguments: jsonOrString(block.FunctionToolCall.Arguments),
			})
		case block.FunctionToolResult != nil:
			result.Parts = append(result.Parts, genAIPart{
				Type:     "tool_call_response",
				ID:       block.FunctionToolResult.CallID,
				Name:     block.FunctionToolResult.Name,
				Response: functionToolResultResponse(block.FunctionToolResult),
			})
		default:
			result.Parts = appendPart(result.Parts, genericBlockPart(block))
		}
	}
	if message.Role == schema.AgenticRoleTypeAssistant {
		if hasToolCall {
			result.FinishReason = "tool_call"
		} else if len(result.Parts) > 0 {
			result.FinishReason = "stop"
		}
	}
	return result, true
}

func functionToolResultResponse(result *schema.FunctionToolResult) any {
	texts := make([]string, 0, len(result.Content))
	for _, block := range result.Content {
		if block == nil {
			continue
		}
		if block.Text == nil {
			return mediaSummary(result.Content)
		}
		texts = append(texts, block.Text.Text)
	}
	return jsonOrString(strings.Join(texts, ""))
}

func mediaSummary(blocks []*schema.FunctionToolResultContentBlock) []genAIPart {
	parts := make([]genAIPart, 0, len(blocks))
	for _, block := range blocks {
		if block == nil {
			continue
		}
		if block.Text != nil {
			parts = append(parts, genAIPart{Type: "text", Content: block.Text.Text})
			continue
		}
		parts = append(parts, genAIPart{Type: "blob", Modality: string(block.Type)})
	}
	return parts
}

// genericBlockPart records blocks without a GenAI equivalent (media, server
// and MCP tool blocks) by type plus URL/MIME metadata, without inline data.
func genericBlockPart(block *schema.ContentBlock) *genAIPart {
	encoded, err := json.Marshal(block)
	if err != nil {
		return &genAIPart{Type: string(block.Type)}
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(encoded, &fields) != nil {
		return &genAIPart{Type: string(block.Type)}
	}
	delete(fields, "type")
	for key, raw := range fields {
		var payload struct {
			URL      string `json:"url"`
			MIMEType string `json:"mime_type"`
			Name     string `json:"name"`
		}
		if json.Unmarshal(raw, &payload) != nil {
			continue
		}
		modality := mediaModality(key)
		if modality == "" {
			return &genAIPart{Type: string(block.Type), Content: raw}
		}
		return uriPart(modality, payload.URL, payload.MIMEType, payload.Name)
	}
	return &genAIPart{Type: string(block.Type)}
}

func mediaModality(field string) string {
	for _, modality := range []string{"image", "audio", "video", "file"} {
		if strings.HasSuffix(field, "_"+modality) {
			return modality
		}
	}
	return ""
}

func convertToolDefinitions(tools []*schema.ToolInfo) []genAIToolDefinition {
	definitions := make([]genAIToolDefinition, 0, len(tools))
	for _, info := range tools {
		if info == nil {
			continue
		}
		definition := genAIToolDefinition{Type: toolTypeFunction, Name: info.Name, Description: info.Desc}
		if info.ParamsOneOf != nil {
			if parameters, err := info.ParamsOneOf.ToJSONSchema(); err == nil && parameters != nil {
				definition.Parameters = parameters
			}
		}
		definitions = append(definitions, definition)
	}
	return definitions
}

// finishReasons collects the provider finish reasons of output messages.
func finishReasons(messages []genAIMessage) []string {
	var reasons []string
	for _, message := range messages {
		if message.FinishReason != "" {
			reasons = append(reasons, message.FinishReason)
		}
	}
	return reasons
}

// reactFinishReason normalizes why a ReAct round ended.
func reactFinishReason(message genAIMessage) string {
	for _, part := range message.Parts {
		if part.Type == "tool_call" {
			return "tool_calls"
		}
	}
	switch reason := strings.ToLower(message.FinishReason); reason {
	case "", "stop", "end_turn", "complete", "completed":
		return "stop"
	case "tool_calls", "tool_call", "tool_use", "function_call":
		return "tool_calls"
	default:
		return reason
	}
}

func toolCallIDs(message genAIMessage) []string {
	var ids []string
	for _, part := range message.Parts {
		if part.Type == "tool_call" && part.ID != "" {
			ids = append(ids, part.ID)
		}
	}
	return ids
}

// finalAssistantMessage returns the last assistant answer without tool calls,
// falling back to the last message.
func finalAssistantMessage(messages []genAIMessage) []genAIMessage {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "assistant" && len(toolCallIDs(messages[i])) == 0 && len(messages[i].Parts) > 0 {
			return messages[i : i+1]
		}
	}
	if len(messages) == 0 {
		return nil
	}
	return messages[len(messages)-1:]
}

// lastUserMessage returns the most recent user message for entry display.
func lastUserMessage(messages []genAIMessage) []genAIMessage {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			return messages[i : i+1]
		}
	}
	return nil
}

func jsonOrString(raw string) any {
	trimmed := strings.TrimSpace(raw)
	if trimmed != "" && (trimmed[0] == '{' || trimmed[0] == '[') && json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed)
	}
	return raw
}
