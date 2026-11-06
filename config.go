// This file contains code adapted from github.com/cloudwego/eino-ext
// (callbacks/langfuse/v2), Copyright 2026 CloudWeGo Authors, licensed under
// the Apache License, Version 2.0. It was modified to emit Tencent Cloud CLS
// Agent Trace spans over OTLP/HTTP instead of Langfuse observations.

package agentobs

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
	"go.opentelemetry.io/otel/trace"
)

// Version is the SDK version reported as the instrumentation scope version.
const Version = "v0.1.0"

const (
	instrumentationName = "git.woa.com/albertbi/tencentcloud-agentobs-sdk-eino-go"

	defaultServiceName             = "eino-agent"
	defaultAgentType               = "eino"
	defaultEntryType               = "sdk"
	defaultTimeout                 = 10 * time.Second
	defaultMaxAttributeValueLength = 256 * 1024
	defaultMaxSpanAttributeBytes   = 1_000_000

	publicDomain   = "cls.tencentcs.com"
	intranetDomain = "cls.tencentyun.com"
	tracesPath     = "/v1/traces"

	// Environment variables used when the corresponding Config field is empty.
	EnvSecretID  = "TENCENTCLOUD_SECRET_ID"
	EnvSecretKey = "TENCENTCLOUD_SECRET_KEY"
	EnvRegion    = "CLS_DEFAULT_REGION"
	EnvTopicID   = "CLS_TOPIC_ID"
)

var (
	regionPattern  = regexp.MustCompile(`^[a-z]+(-[a-z0-9]+)+$`)
	topicIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{0,127}$`)
)

// RetryConfig configures exponential backoff for transient OTLP export
// failures. It is an alias of the OpenTelemetry OTLP/HTTP retry config.
type RetryConfig = otlptracehttp.RetryConfig

// Config configures the CLS Agent Observability callback. Fields are ordered
// for memory alignment; see each comment for its purpose.
type Config struct {
	// SpanExporter replaces the CLS exporter (custom transports and tests).
	// It is still wrapped by the reporting batch processor.
	SpanExporter sdktrace.SpanExporter
	// MaskFunc is applied to every captured content value before export.
	MaskFunc func(string) string
	// TracerProvider lets callers supply an existing provider. It is flushed
	// but never shut down by the handler; its queueing and export failures are
	// reported by its own span processor, not by this SDK.
	TracerProvider *sdktrace.TracerProvider
	// HTTPClient overrides the HTTP client used by the CLS exporter.
	HTTPClient *http.Client
	// RetryConfig overrides the OTLP/HTTP retry policy. Nil uses the
	// OpenTelemetry defaults.
	RetryConfig *RetryConfig
	// Region is the CLS region, for example "ap-guangzhou". Falls back to
	// CLS_DEFAULT_REGION. Ignored when Endpoint is set.
	Region string
	// Environment is the resource deployment.environment.name.
	Environment string
	// CrossRegion sets the x-cross-region header when uploading from another
	// region, for example "ap-guangzhou".
	CrossRegion string
	// SecretID authenticates the upload (HTTP Basic) together with SecretKey.
	// Falls back to TENCENTCLOUD_SECRET_ID. Never hardcode it; use a CAM
	// sub-account restricted to CLS writes.
	SecretID string
	// TopicID is the CLS trace topic ID ({app}-trace-topic). Falls back to
	// CLS_TOPIC_ID.
	TopicID string
	// Endpoint overrides the derived trace URL. A URL without a path gets
	// /v1/traces appended. Only https is accepted unless Insecure is set.
	Endpoint string
	// ServiceName is the resource service.name. Defaults to "eino-agent".
	ServiceName string
	// SecretKey pairs with SecretID. Falls back to TENCENTCLOUD_SECRET_KEY.
	SecretKey string
	// Release is the resource service.version.
	Release string
	// AgentType is gen_ai.agent.type on every span. Defaults to "eino".
	AgentType string
	// EntryType is gen_ai.entry.type on the entry span. Defaults to "sdk".
	EntryType string
	// UserID, UserName and SessionID are defaults for every trace. Prefer the
	// StartTrace options for per-request values.
	UserID    string
	UserName  string
	SessionID string
	// Timeout limits a single OTLP HTTP request. Defaults to 10s.
	Timeout time.Duration
	// BatchTimeout is the maximum delay before a partial batch is exported
	// (default 5s).
	BatchTimeout time.Duration
	// DropLogInterval controls how often truncations and dropped spans are
	// summarized. Zero uses one minute; negative logs every event.
	DropLogInterval time.Duration
	// MaxExportBatchSize is the maximum spans per OTLP request (default 512,
	// must not exceed MaxQueueSize).
	MaxExportBatchSize int
	// MaxQueueSize bounds spans waiting for export (default 2048). Spans that
	// arrive while the queue is full are dropped and counted in the periodic
	// loss log as queue_full_spans.
	MaxQueueSize int
	// MaxAttributeValueLength limits each serialized content value. Zero uses
	// 256 KiB; a negative value disables the limit.
	MaxAttributeValueLength int
	// MaxSpanAttributeBytes limits the keys and values written to one span.
	// Zero uses 1,000,000 bytes; a negative value disables the limit.
	MaxSpanAttributeBytes int
	// SampleRate in (0, 1) samples traces by trace ID. 0 and 1 keep all.
	SampleRate float64
	// IncludeFrameworkSpans keeps Eino framework spans (Graph, Chain, Lambda,
	// ToolsNode, Prompt, ...) as gen_ai.span.kind=task. By default they are
	// dropped and their children attach to the nearest kept parent.
	IncludeFrameworkSpans bool
	// DisableContentCapture omits message contents, tool arguments/results and
	// retrieved documents. Token usage, timings and identifiers are kept.
	DisableContentCapture bool
	// DisableGraphAsAgent stops treating the outermost Graph/Chain/Workflow
	// (when no ADK agent encloses it) as an agent span.
	DisableGraphAsAgent bool
	// DisableStepSpans keeps ReAct step identifiers (gen_ai.step.id,
	// gen_ai.react.round) on chat/tool spans but does not emit step spans.
	DisableStepSpans bool
	// ExportDiagnostics appends protobuf/gzip payload sizes, HTTP attempt
	// counts and DNS/connect/TLS/write/wait-header timings to the log of a
	// final export failure. It never logs headers, credentials, span contents
	// or trace IDs, and adds a small CPU cost (the first request body is
	// re-read). Only applies to the built-in CLS exporter.
	ExportDiagnostics bool
	// IncludeProcessResourceAttributes adds OTel process.* resource attributes.
	IncludeProcessResourceAttributes bool
	// Insecure allows an http:// Endpoint. Use only for local testing.
	Insecure bool
	// Intranet uses the VPC endpoint {region}.cls.tencentyun.com instead of the
	// public {region}.cls.tencentcs.com endpoint.
	Intranet bool
}

// Handler converts Eino callbacks into CLS Agent Trace spans.
type Handler struct {
	tracer                trace.Tracer
	losses                *lossReporter
	prepare               func(string) string
	provider              *sdktrace.TracerProvider
	activeRuns            traceRunRegistry
	defaults              traceOptions
	async                 asyncGroup
	maxSpanAttributeBytes int
	includeFramework      bool
	captureContent        bool
	stepSpans             bool
	graphAsAgent          bool
	ownProvider           bool
}

// NewHandler creates the callback handler. Register it with
// callbacks.AppendGlobalHandlers, compose.WithCallbacks or adk.WithCallbacks.
func NewHandler(ctx context.Context, cfg *Config) (*Handler, error) {
	if cfg == nil {
		return nil, errors.New("agentobs: config is nil")
	}
	if cfg.SampleRate < 0 || cfg.SampleRate > 1 {
		return nil, fmt.Errorf("agentobs: sample rate must be between 0 and 1, got %v", cfg.SampleRate)
	}

	losses := newLossReporter(cfg.DropLogInterval, nil)
	succeeded := false
	defer func() {
		if !succeeded {
			losses.Stop()
		}
	}()

	provider := cfg.TracerProvider
	ownProvider := false
	if provider == nil {
		exporter := cfg.SpanExporter
		exportDiagnostics := false
		if exporter == nil {
			var err error
			exporter, err = newCLSExporter(ctx, cfg)
			if err != nil {
				return nil, err
			}
			exportDiagnostics = cfg.ExportDiagnostics
			defer func() {
				if !succeeded {
					_ = exporter.Shutdown(context.Background())
				}
			}()
		}
		res, err := newResource(ctx, cfg)
		if err != nil {
			return nil, err
		}
		processor, err := newReportingBatchSpanProcessor(exporter, batchProcessorConfig{
			maxQueueSize:       cfg.MaxQueueSize,
			maxExportBatchSize: cfg.MaxExportBatchSize,
			batchTimeout:       cfg.BatchTimeout,
			losses:             losses,
			exportDiagnostics:  exportDiagnostics,
		})
		if err != nil {
			return nil, err
		}
		provider = sdktrace.NewTracerProvider(
			sdktrace.WithIDGenerator(contextIDGenerator{}),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(reportingSampler{delegate: sampler(cfg.SampleRate), losses: losses}),
			sdktrace.WithSpanProcessor(processor),
		)
		ownProvider = true
	}

	maxLength := cfg.MaxAttributeValueLength
	if maxLength == 0 {
		maxLength = defaultMaxAttributeValueLength
	}
	maxSpanAttributeBytes := cfg.MaxSpanAttributeBytes
	if maxSpanAttributeBytes == 0 {
		maxSpanAttributeBytes = defaultMaxSpanAttributeBytes
	}

	handler := &Handler{
		tracer:                provider.Tracer(instrumentationName, trace.WithInstrumentationVersion(Version)),
		provider:              provider,
		ownProvider:           ownProvider,
		prepare:               newValuePreparer(cfg.MaskFunc, maxLength, losses),
		maxSpanAttributeBytes: maxSpanAttributeBytes,
		defaults: traceOptions{
			SessionID: cfg.SessionID,
			UserID:    cfg.UserID,
			UserName:  cfg.UserName,
			AgentType: defaultString(cfg.AgentType, defaultAgentType),
			EntryType: defaultString(cfg.EntryType, defaultEntryType),
		},
		includeFramework: cfg.IncludeFrameworkSpans,
		graphAsAgent:     !cfg.DisableGraphAsAgent,
		stepSpans:        !cfg.DisableStepSpans,
		captureContent:   !cfg.DisableContentCapture,
		async:            newAsyncGroup(),
		losses:           losses,
	}
	succeeded = true
	return handler, nil
}

// Flush waits for callback-owned stream collectors and flushes finished spans.
func (h *Handler) Flush(ctx context.Context) error {
	if err := h.async.wait(ctx, false); err != nil {
		log.Printf("agentobs: flush interrupted while waiting for active callbacks: %v", err)
		return err
	}
	err := h.provider.ForceFlush(ctx)
	if err != nil {
		log.Printf("agentobs: flush failed; telemetry may not have been exported: %v", err)
	}
	return err
}

// Shutdown ends active entry spans, drains stream collectors and shuts down
// the handler-owned provider. Call it before the process exits.
func (h *Handler) Shutdown(ctx context.Context) error {
	defer h.losses.Stop()
	if err := h.async.wait(ctx, true); err != nil {
		log.Printf("agentobs: shutdown interrupted while waiting for active callbacks: %v", err)
		return err
	}
	h.activeRuns.endAll()
	if h.ownProvider {
		return h.provider.Shutdown(ctx)
	}
	return h.provider.ForceFlush(ctx)
}

func newResource(ctx context.Context, cfg *Config) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceName(defaultString(cfg.ServiceName, defaultServiceName))}
	if cfg.Release != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.Release))
	}
	if cfg.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironmentName(cfg.Environment))
	}
	options := []resource.Option{
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		resource.WithAttributes(attrs...),
	}
	if cfg.IncludeProcessResourceAttributes {
		options = append(options, resource.WithProcess())
	}
	res, err := resource.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("agentobs: create resource: %w", err)
	}
	return res, nil
}

// clsTransport is the validated CLS upload target.
type clsTransport struct {
	headers  map[string]string
	endpoint string
}

func resolveCLSTransport(cfg *Config) (*clsTransport, error) {
	endpoint, err := clsTraceEndpoint(cfg)
	if err != nil {
		return nil, err
	}
	topicID := strings.TrimSpace(firstNonEmpty(cfg.TopicID, os.Getenv(EnvTopicID)))
	if topicID == "" {
		return nil, fmt.Errorf("agentobs: CLS topic ID is required (Config.TopicID or %s)", EnvTopicID)
	}
	if !topicIDPattern.MatchString(topicID) {
		return nil, errors.New("agentobs: invalid CLS topic ID")
	}
	secretID := strings.TrimSpace(firstNonEmpty(cfg.SecretID, os.Getenv(EnvSecretID)))
	secretKey := strings.TrimSpace(firstNonEmpty(cfg.SecretKey, os.Getenv(EnvSecretKey)))
	if secretID == "" || secretKey == "" {
		return nil, fmt.Errorf("agentobs: CLS credentials are required (Config.SecretID/SecretKey or %s/%s)", EnvSecretID, EnvSecretKey)
	}
	if strings.ContainsAny(secretID, ":\r\n") || strings.ContainsAny(secretKey, "\r\n") {
		return nil, errors.New("agentobs: invalid CLS credentials")
	}
	headers := map[string]string{
		"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(secretID+":"+secretKey)),
		"topic_id":      topicID,
	}
	if cross := strings.TrimSpace(cfg.CrossRegion); cross != "" {
		if !regionPattern.MatchString(cross) {
			return nil, fmt.Errorf("agentobs: invalid cross region %q", cross)
		}
		headers["x-cross-region"] = cross
	}
	return &clsTransport{endpoint: endpoint, headers: headers}, nil
}

func clsTraceEndpoint(cfg *Config) (string, error) {
	if raw := strings.TrimSpace(cfg.Endpoint); raw != "" {
		parsed, err := url.Parse(raw)
		if err != nil || parsed.Host == "" {
			return "", errors.New("agentobs: invalid CLS endpoint")
		}
		switch parsed.Scheme {
		case "https":
		case "http":
			if !cfg.Insecure {
				return "", errors.New("agentobs: http CLS endpoint requires Config.Insecure (testing only)")
			}
		default:
			return "", fmt.Errorf("agentobs: unsupported CLS endpoint scheme %q", parsed.Scheme)
		}
		if parsed.User != nil {
			return "", errors.New("agentobs: CLS endpoint must not contain credentials")
		}
		if parsed.Path == "" || parsed.Path == "/" {
			parsed.Path = tracesPath
		}
		parsed.RawQuery, parsed.Fragment = "", ""
		return parsed.String(), nil
	}
	region := strings.TrimSpace(firstNonEmpty(cfg.Region, os.Getenv(EnvRegion)))
	if region == "" {
		return "", fmt.Errorf("agentobs: CLS region is required (Config.Region or %s)", EnvRegion)
	}
	if !regionPattern.MatchString(region) {
		return "", fmt.Errorf("agentobs: invalid CLS region %q", region)
	}
	domain := publicDomain
	if cfg.Intranet {
		domain = intranetDomain
	}
	return "https://" + region + "." + domain + tracesPath, nil
}

func newCLSExporter(ctx context.Context, cfg *Config) (sdktrace.SpanExporter, error) {
	transport, err := resolveCLSTransport(cfg)
	if err != nil {
		return nil, err
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultTimeout
	}
	options := []otlptracehttp.Option{
		otlptracehttp.WithEndpointURL(transport.endpoint),
		otlptracehttp.WithHeaders(transport.headers),
		otlptracehttp.WithTimeout(timeout),
		otlptracehttp.WithCompression(otlptracehttp.GzipCompression),
	}
	if cfg.RetryConfig != nil {
		options = append(options, otlptracehttp.WithRetry(*cfg.RetryConfig))
	}
	if cfg.ExportDiagnostics {
		options = append(options, otlptracehttp.WithHTTPClient(newDiagnosticHTTPClient(cfg.HTTPClient, timeout)))
	} else if cfg.HTTPClient != nil {
		options = append(options, otlptracehttp.WithHTTPClient(cfg.HTTPClient))
	}
	exporter, err := otlptracehttp.New(ctx, options...)
	if err != nil {
		return nil, fmt.Errorf("agentobs: create CLS OTLP exporter: %w", err)
	}
	return exporter, nil
}

func sampler(sampleRate float64) sdktrace.Sampler {
	if sampleRate <= 0 || sampleRate >= 1 {
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(sampleRate))
}

type reportingSampler struct {
	delegate sdktrace.Sampler
	losses   *lossReporter
}

func (s reportingSampler) ShouldSample(parameters sdktrace.SamplingParameters) sdktrace.SamplingResult {
	result := s.delegate.ShouldSample(parameters)
	if result.Decision == sdktrace.Drop {
		s.losses.Add(lossSampledOut, 1)
	}
	return result
}

func (s reportingSampler) Description() string {
	return "agentobs-reporting(" + s.delegate.Description() + ")"
}

func defaultString(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
