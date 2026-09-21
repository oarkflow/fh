package transport_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/debug"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/runtime"
	refcli "github.com/oarkflow/fh/ref/transport/cli"
	refgrpc "github.com/oarkflow/fh/ref/transport/grpc"
	refqueue "github.com/oarkflow/fh/ref/transport/queue"
	refws "github.com/oarkflow/fh/ref/transport/websocket"
)

type EchoInput struct {
	Message string `json:"message"`
}

type EchoOutput struct {
	Echo string `json:"echo"`
}

type EchoIntent struct{}

func (EchoIntent) Name() intent.Name { return "echo" }
func (EchoIntent) Spec() intent.Spec {
	return intent.Spec{
		Description: "Echoes message back",
		Requires:    []fact.AnyKey{capability.TraceKey.Any()},
	}
}

func (EchoIntent) Run(nc *execution.NodeContext, in EchoInput) (intent.Outcome[EchoOutput], error) {
	if in.Message == "fail" {
		return intent.Outcome[EchoOutput]{}, intent.Failure{
			Code:     "INVALID_MESSAGE",
			Category: intent.CategoryInvalidInput,
			Message:  "message cannot be fail",
		}
	}
	return intent.Outcome[EchoOutput]{
		Value: EchoOutput{Echo: in.Message},
		Effects: effect.EffectPlan{
			LocalTx: []effect.Effect{},
		},
	}, nil
}

func setupEngine(t *testing.T) *runtime.Engine {
	e := runtime.NewEngine(
		runtime.WithCapability(capability.NewTraceCapability("trace.default", nil)),
	)
	if err := intent.Register(e.Intents(), EchoIntent{}); err != nil {
		t.Fatalf("failed to register EchoIntent: %v", err)
	}
	if err := e.Compile(); err != nil {
		t.Fatalf("failed to compile engine: %v", err)
	}
	return e
}

func TestGRPCTransport(t *testing.T) {
	e := setupEngine(t)
	handler := refgrpc.UnaryHandler(e, "echo")

	reqBytes, _ := json.Marshal(EchoInput{Message: "hello grpc"})
	respBytes, err := handler(context.Background(), reqBytes, map[string][]string{})
	if err != nil {
		t.Fatalf("grpc call failed: %v", err)
	}

	var out EchoOutput
	if err := json.Unmarshal(respBytes, &out); err != nil {
		t.Fatalf("failed to unmarshal grpc response: %v", err)
	}
	if out.Echo != "hello grpc" {
		t.Errorf("expected hello grpc, got %s", out.Echo)
	}

	// Test error mapping
	reqFail, _ := json.Marshal(EchoInput{Message: "fail"})
	_, err = handler(context.Background(), reqFail, nil)
	if err == nil {
		t.Fatalf("expected rpc error on fail message, got nil")
	}
}

func TestWebSocketTransport(t *testing.T) {
	e := setupEngine(t)
	handler := refws.Handler(e)

	msg := refws.Message{
		ID:      "ws-1",
		Intent:  "echo",
		Payload: json.RawMessage(`{"message":"hello ws"}`),
	}

	resp, err := handler(context.Background(), "conn-100", msg)
	if err != nil {
		t.Fatalf("ws handler error: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success, got error %s", resp.Error)
	}
}

func TestQueueTransport(t *testing.T) {
	e := setupEngine(t)
	consumer := refqueue.Consumer(e, "echo")

	err := consumer(context.Background(), "orders", []byte(`{"message":"hello queue"}`), map[string]string{
		"message_id": "msg-99",
	})
	if err != nil {
		t.Fatalf("queue consumption failed: %v", err)
	}
}

func TestCLITransport(t *testing.T) {
	e := setupEngine(t)
	cmd := refcli.Command(e, "echo")

	out, err := cmd(context.Background(), []string{"--dry-run"}, []byte(`{"message":"hello cli"}`))
	if err != nil {
		t.Fatalf("cli execution failed: %v", err)
	}

	var parsed EchoOutput
	if err := json.Unmarshal(out, &parsed); err != nil {
		t.Fatalf("failed to parse cli output JSON: %v", err)
	}
	if parsed.Echo != "hello cli" {
		t.Errorf("expected hello cli, got %s", parsed.Echo)
	}
}

func TestDebugIntrospection(t *testing.T) {
	e := setupEngine(t)
	plan, ok := e.Plan("echo")
	if !ok {
		t.Fatalf("plan echo not found")
	}

	summary := debug.InspectPlan(plan)
	if summary.IntentName != "echo" || summary.NodeCount != 3 {
		t.Errorf("unexpected summary: %+v", summary)
	}

	mermaid := debug.ToMermaid(plan)
	if len(mermaid) == 0 {
		t.Errorf("expected non-empty Mermaid graph")
	}

	dot := debug.ToDOT(plan)
	if len(dot) == 0 {
		t.Errorf("expected non-empty DOT graph")
	}
}
