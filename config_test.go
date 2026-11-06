package agentobs

import (
	"compress/gzip"
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

type capturedRequest struct {
	path    string
	headers http.Header
	request *collectortrace.ExportTraceServiceRequest
}

func TestCLSExporterRequest(t *testing.T) {
	var mu sync.Mutex
	var captured []capturedRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			reader, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			defer reader.Close()
			body = reader
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		decoded := &collectortrace.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(raw, decoded); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		captured = append(captured, capturedRequest{path: r.URL.Path, headers: r.Header.Clone(), request: decoded})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	secretID, secretKey := "test-id", "test-key-value"
	handler, err := NewHandler(context.Background(), &Config{
		Endpoint:     server.URL,
		Insecure:     true,
		TopicID:      "abc-trace-topic",
		SecretID:     secretID,
		SecretKey:    secretKey,
		CrossRegion:  "ap-guangzhou",
		ServiceName:  "my-agent",
		Environment:  "test",
		Release:      "1.2.3",
		BatchTimeout: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := handler.StartTrace(context.Background(), WithInput("ping"))
	handler.EndTrace(ctx, "pong")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := handler.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(captured) == 0 {
		t.Fatal("no export request received")
	}
	got := captured[0]
	if got.path != "/v1/traces" {
		t.Errorf("path = %q", got.path)
	}
	wantAuth := "Basic " + base64.StdEncoding.EncodeToString([]byte(secretID+":"+secretKey))
	if got.headers.Get("Authorization") != wantAuth {
		t.Errorf("authorization header mismatch")
	}
	if got.headers.Get("topic_id") != "abc-trace-topic" {
		t.Errorf("topic_id = %q", got.headers.Get("topic_id"))
	}
	if got.headers.Get("x-cross-region") != "ap-guangzhou" {
		t.Errorf("x-cross-region = %q", got.headers.Get("x-cross-region"))
	}
	if got.headers.Get("Content-Encoding") != "gzip" || got.headers.Get("Content-Type") != "application/x-protobuf" {
		t.Errorf("encoding headers = %q / %q", got.headers.Get("Content-Encoding"), got.headers.Get("Content-Type"))
	}

	resourceSpans := got.request.GetResourceSpans()
	if len(resourceSpans) != 1 {
		t.Fatalf("resource spans = %d", len(resourceSpans))
	}
	resourceAttrs := map[string]string{}
	for _, kv := range resourceSpans[0].GetResource().GetAttributes() {
		resourceAttrs[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	if resourceAttrs["service.name"] != "my-agent" || resourceAttrs["service.version"] != "1.2.3" || resourceAttrs["deployment.environment.name"] != "test" {
		t.Errorf("resource attributes = %v", resourceAttrs)
	}
	scopeSpans := resourceSpans[0].GetScopeSpans()
	if len(scopeSpans) != 1 || scopeSpans[0].GetScope().GetName() != instrumentationName || len(scopeSpans[0].GetSpans()) != 1 {
		t.Fatalf("unexpected scope spans: %v", scopeSpans)
	}
	span := scopeSpans[0].GetSpans()[0]
	if span.GetName() != OperationEnterApplication {
		t.Errorf("span name = %q", span.GetName())
	}
	spanAttrs := map[string]string{}
	for _, kv := range span.GetAttributes() {
		spanAttrs[kv.GetKey()] = kv.GetValue().GetStringValue()
	}
	if spanAttrs[attrSpanKind] != SpanKindEntry || !strings.Contains(spanAttrs[attrOutputMessages], "pong") {
		t.Errorf("span attributes = %v", spanAttrs)
	}
}

func TestResolveCLSTransport(t *testing.T) {
	valid := func() *Config {
		return &Config{Region: "ap-guangzhou", TopicID: "topic-1", SecretID: "id", SecretKey: "key"}
	}
	t.Run("public endpoint", func(t *testing.T) {
		transport, err := resolveCLSTransport(valid())
		if err != nil {
			t.Fatal(err)
		}
		if transport.endpoint != "https://ap-guangzhou.cls.tencentcs.com/v1/traces" {
			t.Errorf("endpoint = %s", transport.endpoint)
		}
		if _, ok := transport.headers["x-cross-region"]; ok {
			t.Errorf("unexpected cross region header")
		}
	})
	t.Run("intranet endpoint", func(t *testing.T) {
		cfg := valid()
		cfg.Intranet = true
		transport, err := resolveCLSTransport(cfg)
		if err != nil || transport.endpoint != "https://ap-guangzhou.cls.tencentyun.com/v1/traces" {
			t.Errorf("endpoint = %v, err = %v", transport, err)
		}
	})
	t.Run("custom endpoint keeps path and drops query", func(t *testing.T) {
		cfg := valid()
		cfg.Endpoint = "https://proxy.example.com/custom/traces?x=1#frag"
		transport, err := resolveCLSTransport(cfg)
		if err != nil || transport.endpoint != "https://proxy.example.com/custom/traces" {
			t.Errorf("endpoint = %v, err = %v", transport, err)
		}
	})
	t.Run("environment fallback", func(t *testing.T) {
		t.Setenv(EnvRegion, "ap-shanghai")
		t.Setenv(EnvTopicID, "env-topic")
		t.Setenv(EnvSecretID, "env-id")
		t.Setenv(EnvSecretKey, "env-key")
		transport, err := resolveCLSTransport(&Config{})
		if err != nil {
			t.Fatal(err)
		}
		if transport.endpoint != "https://ap-shanghai.cls.tencentcs.com/v1/traces" || transport.headers["topic_id"] != "env-topic" {
			t.Errorf("transport = %+v", transport.endpoint)
		}
		if transport.headers["Authorization"] != "Basic "+base64.StdEncoding.EncodeToString([]byte("env-id:env-key")) {
			t.Errorf("authorization mismatch")
		}
	})

	for name, mutate := range map[string]func(*Config){
		"missing region":        func(c *Config) { c.Region = "" },
		"invalid region":        func(c *Config) { c.Region = "evil.com/x" },
		"missing topic":         func(c *Config) { c.TopicID = "" },
		"invalid topic":         func(c *Config) { c.TopicID = "a\r\nb" },
		"missing secret key":    func(c *Config) { c.SecretKey = "" },
		"colon in secret id":    func(c *Config) { c.SecretID = "a:b" },
		"newline in secret key": func(c *Config) { c.SecretKey = "a\nb" },
		"bad cross region":      func(c *Config) { c.CrossRegion = "x\r\ny" },
		"http without insecure": func(c *Config) { c.Endpoint = "http://localhost:4318" },
		"unsupported scheme":    func(c *Config) { c.Endpoint = "ftp://example.com" },
		"credentials in url":    func(c *Config) { c.Endpoint = "https://user:pass@example.com" },
		"missing host":          func(c *Config) { c.Endpoint = "https://" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvRegion, "")
			t.Setenv(EnvTopicID, "")
			t.Setenv(EnvSecretID, "")
			t.Setenv(EnvSecretKey, "")
			cfg := valid()
			mutate(cfg)
			if _, err := resolveCLSTransport(cfg); err == nil {
				t.Errorf("expected error")
			}
		})
	}
}

func TestNewHandlerValidation(t *testing.T) {
	if _, err := NewHandler(context.Background(), nil); err == nil {
		t.Error("nil config accepted")
	}
	if _, err := NewHandler(context.Background(), &Config{SampleRate: 1.5}); err == nil {
		t.Error("invalid sample rate accepted")
	}
	t.Setenv(EnvTopicID, "")
	if _, err := NewHandler(context.Background(), &Config{Region: "ap-guangzhou", SecretID: "a", SecretKey: "b"}); err == nil {
		t.Error("missing topic accepted")
	}
}

func TestErrorsDoNotLeakSecrets(t *testing.T) {
	cfg := &Config{Region: "ap-guangzhou", SecretID: "AKIDsecretvalue", SecretKey: "topsecretkey", TopicID: "bad topic"}
	_, err := resolveCLSTransport(cfg)
	if err == nil {
		t.Fatal("expected error")
	}
	if strings.Contains(err.Error(), "AKIDsecretvalue") || strings.Contains(err.Error(), "topsecretkey") || strings.Contains(err.Error(), "bad topic") {
		t.Errorf("error leaks configuration: %v", err)
	}
}

func TestSampling(t *testing.T) {
	env := newTestEnv(t, func(c *Config) { c.SampleRate = 0.000001 })
	kept := 0
	for i := 0; i < 50; i++ {
		ctx := env.handler.StartTrace(context.Background())
		env.handler.EndTrace(ctx, "")
	}
	kept = len(env.spans())
	if kept > 5 {
		t.Errorf("sampling kept %d of 50 traces", kept)
	}
}
