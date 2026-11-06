// This file contains code adapted from github.com/cloudwego/eino-ext
// (callbacks/langfuse/v2), Copyright 2026 CloudWeGo Authors, licensed under
// the Apache License, Version 2.0. It was modified to emit Tencent Cloud CLS
// Agent Trace spans over OTLP/HTTP instead of Langfuse observations.

package agentobs

import (
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const defaultDropLogInterval = time.Minute

const (
	lossQueueFull             = "queue_full_spans"
	lossProcessorStopped      = "processor_stopped_spans"
	lossSampledOut            = "sampled_out_spans"
	lossValueLimit            = "value_limit_truncations"
	lossSpanAttributeBudget   = "span_attribute_budget_drops_or_truncations"
	lossOTelDroppedAttributes = "otel_dropped_attributes"
	lossOTelDroppedEvents     = "otel_dropped_events"
	lossOTelDroppedLinks      = "otel_dropped_links"
	lossPayloadSerialization  = "payload_serialization_failures"
	lossMetadataSerialization = "metadata_serialization_failures"
)

// lossReporter aggregates telemetry that was dropped or truncated so it is
// never discarded silently, without logging on every event.
type lossReporter struct {
	counts   map[string]uint64
	report   func(map[string]uint64, time.Duration)
	stopCh   chan struct{}
	doneCh   chan struct{}
	interval time.Duration
	stopOnce sync.Once
	mu       sync.Mutex
	stopped  bool
}

func newLossReporter(interval time.Duration, report func(map[string]uint64, time.Duration)) *lossReporter {
	if interval == 0 {
		interval = defaultDropLogInterval
	}
	if report == nil {
		report = logTelemetryLosses
	}
	r := &lossReporter{
		counts:   make(map[string]uint64),
		interval: interval,
		report:   report,
		stopCh:   make(chan struct{}),
		doneCh:   make(chan struct{}),
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				_ = recoveredPanic("telemetry loss reporter", recovered)
			}
		}()
		r.run()
	}()
	return r
}

func (r *lossReporter) Add(reason string, count uint64) {
	if r == nil || count == 0 {
		return
	}
	r.mu.Lock()
	if r.interval > 0 && !r.stopped {
		r.counts[reason] += count
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	r.report(map[string]uint64{reason: count}, r.interval)
}

func (r *lossReporter) Stop() {
	if r == nil {
		return
	}
	r.stopOnce.Do(func() {
		r.mu.Lock()
		r.stopped = true
		r.mu.Unlock()
		close(r.stopCh)
		<-r.doneCh
	})
}

func (r *lossReporter) run() {
	defer close(r.doneCh)
	if r.interval < 0 {
		<-r.stopCh
		return
	}
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			r.flush()
		case <-r.stopCh:
			r.flush()
			return
		}
	}
}

func (r *lossReporter) flush() {
	r.mu.Lock()
	if len(r.counts) == 0 {
		r.mu.Unlock()
		return
	}
	counts := r.counts
	r.counts = make(map[string]uint64)
	r.mu.Unlock()
	r.report(counts, r.interval)
}

func logTelemetryLosses(counts map[string]uint64, interval time.Duration) {
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strconv.FormatUint(counts[key], 10))
	}
	log.Printf("agentobs: discarded or truncated telemetry since previous report (interval %s): %s", interval, strings.Join(parts, " "))
}

func recoveredPanic(operation string, recovered any) error {
	err := fmt.Errorf("%s panic: %v", operation, recovered)
	log.Printf("agentobs: recovered callback panic: %v\n%s", err, debug.Stack())
	return err
}
