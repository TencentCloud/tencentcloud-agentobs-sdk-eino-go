// This file contains code adapted from github.com/cloudwego/eino-ext
// (callbacks/langfuse/v2), Copyright 2026 CloudWeGo Authors, licensed under
// the Apache License, Version 2.0. It was modified to emit Tencent Cloud CLS
// Agent Trace spans over OTLP/HTTP instead of Langfuse observations.

package agentobs

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// spanAttributeWriter writes attributes under a per-span byte budget so a
// single span never exceeds the CLS log size limits. Content values pass
// through the mask/length preparer; identifiers and enums do not.
type spanAttributeWriter struct {
	mu        sync.Mutex
	span      trace.Span
	prepare   func(string) string
	remaining int
	losses    *lossReporter
}

func newSpanAttributeWriter(span trace.Span, prepare func(string) string, budget int, losses *lossReporter) *spanAttributeWriter {
	return &spanAttributeWriter{span: span, prepare: prepare, remaining: budget, losses: losses}
}

// setContent masks, length-limits and writes a content value. It returns the
// value actually written.
func (w *spanAttributeWriter) setContent(key, value string) string {
	if value == "" {
		return ""
	}
	return w.setString(key, w.prepare(value))
}

// setJSONContent serializes value and writes it as content.
func (w *spanAttributeWriter) setJSONContent(key string, value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		w.losses.Add(lossPayloadSerialization, 1)
		return ""
	}
	return w.setContent(key, string(encoded))
}

// setString writes a non-content value under the span budget.
func (w *spanAttributeWriter) setString(key, value string) string {
	if value == "" {
		return ""
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.remaining >= 0 {
		available := w.remaining - len(key)
		if available <= 0 {
			w.losses.Add(lossSpanAttributeBudget, 1)
			return ""
		}
		if len(value) > available {
			w.losses.Add(lossSpanAttributeBudget, 1)
			value = truncateValue(value, available)
		}
		w.remaining -= len(key) + len(value)
	}
	if value != "" {
		w.span.SetAttributes(attribute.String(key, value))
	}
	return value
}

func (w *spanAttributeWriter) setInt(key string, value int) {
	if !w.reserve(len(key) + len(strconv.Itoa(value))) {
		return
	}
	w.span.SetAttributes(attribute.Int(key, value))
}

func (w *spanAttributeWriter) setInt64(key string, value int64) {
	if !w.reserve(len(key) + len(strconv.FormatInt(value, 10))) {
		return
	}
	w.span.SetAttributes(attribute.Int64(key, value))
}

func (w *spanAttributeWriter) setFloat(key string, value float64) {
	if !w.reserve(len(key) + 8) {
		return
	}
	w.span.SetAttributes(attribute.Float64(key, value))
}

func (w *spanAttributeWriter) setBool(key string, value bool) {
	if !w.reserve(len(key) + 5) {
		return
	}
	w.span.SetAttributes(attribute.Bool(key, value))
}

func (w *spanAttributeWriter) setStringSlice(key string, values []string) {
	if len(values) == 0 {
		return
	}
	size := len(key)
	for _, value := range values {
		size += len(value)
	}
	if !w.reserve(size) {
		return
	}
	w.span.SetAttributes(attribute.StringSlice(key, values))
}

func (w *spanAttributeWriter) setAttributes(attrs []attribute.KeyValue) {
	for _, attr := range attrs {
		switch attr.Value.Type() {
		case attribute.STRING:
			w.setString(string(attr.Key), attr.Value.AsString())
		default:
			if w.reserve(len(attr.Key) + len(attr.Value.Emit())) {
				w.span.SetAttributes(attr)
			}
		}
	}
}

func (w *spanAttributeWriter) reserve(size int) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.remaining < 0 {
		return true
	}
	if size > w.remaining {
		w.losses.Add(lossSpanAttributeBudget, 1)
		return false
	}
	w.remaining -= size
	return true
}

func newValuePreparer(mask func(string) string, maxLength int, losses *lossReporter) func(string) string {
	return func(value string) string {
		if mask != nil {
			value = mask(value)
		}
		if maxLength < 0 || len(value) <= maxLength {
			return value
		}
		losses.Add(lossValueLimit, 1)
		return truncateValue(value, maxLength)
	}
}

// truncateValue keeps JSON values valid by replacing them with a truncation
// envelope; plain text keeps its head and tail.
func truncateValue(value string, maxLength int) string {
	if json.Valid([]byte(value)) {
		return truncateJSON(value, maxLength)
	}
	return truncateText(value, maxLength)
}

func truncateJSON(value string, maxLength int) string {
	if maxLength < 2 {
		return ""
	}
	originalLength := len(value)
	previewLength := maxLength
	for previewLength >= 0 {
		encoded, err := json.Marshal(map[string]any{
			"truncated":      true,
			"original_bytes": originalLength,
			"preview":        truncateText(value, previewLength),
		})
		if err == nil && len(encoded) <= maxLength {
			return string(encoded)
		}
		if previewLength == 0 {
			break
		}
		previewLength /= 2
	}
	return "{}"
}

func truncateText(value string, maxLength int) string {
	if maxLength <= 0 {
		return ""
	}
	if len(value) <= maxLength {
		return value
	}
	const marker = "...[truncated]..."
	if maxLength <= len(marker) {
		return utf8Prefix(value, maxLength)
	}
	available := maxLength - len(marker)
	headLength := available * 7 / 10
	return utf8Prefix(value, headLength) + marker + utf8Suffix(value, available-headLength)
}

func utf8Prefix(value string, maxLength int) string {
	if len(value) <= maxLength {
		return value
	}
	cut := maxLength
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

func utf8Suffix(value string, maxLength int) string {
	if len(value) <= maxLength {
		return value
	}
	start := len(value) - maxLength
	for start < len(value) && !utf8.RuneStart(value[start]) {
		start++
	}
	return value[start:]
}

// contentHash is a non-security fingerprint used by CLS to correlate or
// de-duplicate identical inputs. It is computed over the masked value.
func contentHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:16])
}
