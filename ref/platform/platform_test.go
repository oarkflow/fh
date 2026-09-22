package platform_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/platform"
)

const application = `
name "test-platform"

resource "messages" {
  kind "test-resource"
  config {
    prefix env.required("PLATFORM_TEST_PREFIX")
  }
}

intent "hello" {
  description "Build a greeting without an application handler"
  response "response"

  node "greet" {
    uses "test-greeting"
    resource "messages"
    kind pure
    speculation pre_auth_safe
    requires [input]
    provides [greeting]
  }

  node "response" {
    uses "collect"
    kind pure
    speculation pre_auth_safe
    requires [greeting]
    provides [response]
  }
}

route "hello.show" {
  method POST
  path "/hello/:name"
  intent "hello"
  status 201
  cache_control "no-store"
  headers {
    "X-Platform" "bcl"
  }
}
`

type closeFlag struct{ closed *atomic.Bool }

func (c closeFlag) Close() error { c.closed.Store(true); return nil }

func TestBCLPlatformCompilesExecutesMountsAndCloses(t *testing.T) {
	t.Setenv("PLATFORM_TEST_PREFIX", "Hello")
	registry := platform.NewRegistry()
	closed := &atomic.Bool{}
	if err := registry.RegisterResource("test-resource", platform.ResourceFactoryFunc(
		func(_ context.Context, spec platform.ResourceSpec) (platform.Resource, io.Closer, error) {
			return spec.Config["prefix"], closeFlag{closed: closed}, nil
		},
	)); err != nil {
		t.Fatal(err)
	}
	if err := registry.RegisterAction("test-greeting", platform.ActionFactoryFunc(
		func(_ platform.BuildContext, _ platform.NodeSpec) (platform.Action, error) {
			return platform.ActionFunc(func(ctx *platform.ActionContext) (platform.ActionResult, error) {
				input := ctx.Inputs["input"].(map[string]any)
				value := ctx.Resource.(string) + ", " + input["name"].(string)
				return platform.ActionResult{Outputs: map[string]any{"greeting": value}}, nil
			}), nil
		},
	)); err != nil {
		t.Fatal(err)
	}

	opts := platform.DefaultLoadOptions()
	opts.Registry = registry
	p, err := platform.Compile(context.Background(), []byte(application), t.TempDir(), opts)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	app := fh.NewFast()
	if err := p.Mount(app); err != nil {
		t.Fatalf("mount: %v", err)
	}
	req := httptest.NewRequest("POST", "/hello/Ada?lang=en", bytes.NewBufferString(`{"name":"Ada"}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 201 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("X-Platform"); got != "bcl" {
		t.Fatalf("X-Platform = %q", got)
	}
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("Cache-Control = %q", got)
	}
	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body["greeting"] != "Hello, Ada" {
		t.Fatalf("response = %#v", body)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !closed.Load() {
		t.Fatal("resource was not closed")
	}
}

func TestEnvironmentAccessCanBeDisabled(t *testing.T) {
	opts := platform.DefaultLoadOptions()
	opts.AllowEnv = false
	_, err := platform.Compile(context.Background(), []byte(application), t.TempDir(), opts)
	if err == nil || !strings.Contains(err.Error(), "requires AllowEnv capability") {
		t.Fatalf("expected disabled environment error, got %v", err)
	}
}

func TestResolvedResourceSecretsAreRedactedFromPublicDocument(t *testing.T) {
	t.Setenv("PLATFORM_TEST_SECRET", "do-not-expose-this-api-key")
	src := `
name "redaction"
resource "auth" { kind "auth.api_key" config { key env.required("PLATFORM_TEST_SECRET") principal_id "svc" } }
intent "noop" { response "result" node "result" { uses "constant" provides [result] config { value true } } }
`
	p, err := platform.Compile(context.Background(), []byte(src), t.TempDir(), platform.DefaultLoadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if got := p.Document.Resources[0].Config["key"]; got != "[REDACTED]" {
		t.Fatalf("public resource key = %#v", got)
	}
}

func TestUnknownConnectorFailsBeforeServing(t *testing.T) {
	src := `
name "invalid"
resource "db" { kind "not-registered" }
intent "noop" {
  response "result"
  node "result" { uses "constant" provides [result] config { value true } }
}
`
	opts := platform.DefaultLoadOptions()
	_, err := platform.Compile(context.Background(), []byte(src), t.TempDir(), opts)
	if err == nil || !strings.Contains(err.Error(), `unregistered kind "not-registered"`) {
		t.Fatalf("expected connector error, got %v", err)
	}
}

func TestDeclarativeDenyUsesREFDecisionGate(t *testing.T) {
	src := `
name "policy"
intent "blocked" {
  response "result"
  node "policy" {
    uses "deny"
    kind decision
    provides [result]
    config { message "blocked by BCL policy" }
  }
}
route "blocked" { method GET path "/blocked" intent "blocked" }
`
	opts := platform.DefaultLoadOptions()
	p, err := platform.Compile(context.Background(), []byte(src), t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	app := fh.NewFast()
	if err := p.Mount(app); err != nil {
		t.Fatal(err)
	}
	resp, err := app.Test(httptest.NewRequest("GET", "/blocked", nil))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("status = %d, want 403", resp.StatusCode)
	}
}

func TestBCLExpressionActionUsesDecodedInput(t *testing.T) {
	src := `
name "expressions"
intent "double" {
  response "result"
  node "calculate" {
    uses "expression"
    kind pure
    speculation pre_auth_safe
    requires [input]
    provides [result]
    config { expression "input.amount * 2" }
  }
}
`
	opts := platform.DefaultLoadOptions()
	p, err := platform.Compile(context.Background(), []byte(src), t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	result, err := p.Engine.Dispatch(context.Background(), &invocation.Invocation{
		Intent: "double",
		Input:  invocation.NewInput([]byte(`{"amount":21}`), "application/json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Value != float64(42) {
		t.Fatalf("value = %#v, want 42", result.Value)
	}
}

func TestProductionBuiltinsAuthenticateCacheAndSession(t *testing.T) {
	src := fmt.Sprintf(`
name "production"
resource "auth" { kind "auth.api_key" config { key "secret-key-with-enough-entropy" principal_id "svc" roles [admin] } }
resource "cache" { kind "cache.file" config { dir %q gc_interval 1m } }
resource "sessions" { kind "session.file" config { dir %q secret "0123456789abcdef0123456789abcdef" cookie "sid" max_age 1h } }
intent "profile" {
  response "response"
  node "auth" { uses "auth.authenticate" resource "auth" kind decision provides [principal] }
  node "key" { uses "expression" kind pure speculation pre_auth_safe requires [input] provides [key] config { expression "input.id" } }
  node "store" { uses "cache.set" resource "cache" kind effect requires [key, principal] provides [stored] config { key_fact "key" value_fact "principal" ttl 1m } }
  node "load" { uses "cache.get" resource "cache" kind read requires [key, stored] provides [profile] config { key_fact "key" } }
  node "session" { uses "session.set" resource "sessions" kind effect requires [principal] provides [saved] config { key "principal" value_fact "principal" } }
  node "response" { uses "collect" kind pure requires [profile, saved] provides [response] }
}
route "profile" { method POST path "/profile" intent "profile" session "sessions" }
`, t.TempDir(), t.TempDir())
	p, err := platform.Compile(context.Background(), []byte(src), t.TempDir(), platform.DefaultLoadOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	app := fh.NewFast()
	if err := p.Mount(app); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest("POST", "/profile", strings.NewReader(`{"id":"42"}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-API-Key", "secret-key-with-enough-entropy")
	response, err := app.Test(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("status = %d", response.StatusCode)
	}
	if response.Header.Get("Set-Cookie") == "" {
		t.Fatal("signed session cookie was not written")
	}
	var body map[string]any
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	profile, ok := body["profile"].(map[string]any)
	if !ok || profile["found"] != true {
		t.Fatalf("cache result = %#v", body["profile"])
	}
}

func TestDurableQueueWorkerRunsConfiguredIntent(t *testing.T) {
	done := make(chan struct{}, 1)
	registry := platform.NewRegistry()
	if err := registry.RegisterAction("test.observe", platform.ActionFactoryFunc(func(_ platform.BuildContext, spec platform.NodeSpec) (platform.Action, error) {
		return platform.ActionFunc(func(*platform.ActionContext) (platform.ActionResult, error) {
			select {
			case done <- struct{}{}:
			default:
			}
			return platform.ActionResult{Outputs: map[string]any{spec.Provides[0]: true}}, nil
		}), nil
	})); err != nil {
		t.Fatal(err)
	}
	src := fmt.Sprintf(`
name "workers"
resource "jobs" { kind "queue.file" config { dir %q workers 1 poll_interval 10ms backoff 10ms } }
intent "consume" { response "result" node "observe" { uses "test.observe" kind effect requires [input] provides [result] } }
worker "consumer" { queue "jobs" job_type "platform.intent.consume" intent "consume" }
route "enqueue" { method POST path "/jobs" intent "consume" mode async queue "jobs" }
`, t.TempDir())
	opts := platform.DefaultLoadOptions()
	opts.Registry = registry
	p, err := platform.Compile(context.Background(), []byte(src), t.TempDir(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	app := fh.NewFast()
	if err := p.Mount(app); err != nil {
		t.Fatal(err)
	}
	response, err := app.Test(httptest.NewRequest("POST", "/jobs", strings.NewReader(`{"event":"created"}`)))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 202 {
		t.Fatalf("status = %d", response.StatusCode)
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("configured worker did not execute queued intent")
	}
}
