package http_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/capability"
	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
	refhttp "github.com/oarkflow/fh/ref/transport/http"
)

type ItemInput struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type ItemOutput struct {
	SKU    string `json:"sku"`
	Status string `json:"status"`
}

type ItemIntent struct{}

func (ItemIntent) Name() intent.Name { return "items.create" }
func (ItemIntent) Spec() intent.Spec {
	return intent.Spec{
		Requires: []fact.AnyKey{capability.PrincipalKey.Any()},
	}
}

func (ItemIntent) Run(nc *execution.NodeContext, in ItemInput) (intent.Outcome[ItemOutput], error) {
	if in.Name == "" {
		return intent.Outcome[ItemOutput]{}, intent.Failure{
			Code:     "NAME_REQUIRED",
			Category: intent.CategoryInvalidInput,
			Message:  "name must not be empty",
		}
	}

	p, err := execution.Require(nc, capability.PrincipalKey)
	if err != nil {
		return intent.Outcome[ItemOutput]{}, intent.Failure{
			Code:     "UNAUTHENTICATED",
			Category: intent.CategoryAuth,
			Message:  "principal missing",
		}
	}

	return intent.Outcome[ItemOutput]{
		Value: ItemOutput{
			SKU:    "sku-" + p.ID + "-" + in.Name,
			Status: "created",
		},
		Effects: effect.EffectPlan{
			LocalTx: []effect.Effect{},
		},
		Meta: intent.OutcomeMeta{
			CacheControl: "private, max-age=60",
		},
	}, nil
}

func TestHTTPAdapter(t *testing.T) {
	engine := runtime.NewEngine(
		runtime.WithCapability(capability.NewAuthCapability("auth.token", func(hint invocation.PrincipalHint) (capability.PrincipalFact, error) {
			if hint.BearerToken == "valid-user" {
				return capability.PrincipalFact{ID: "usr-99", Username: "tester"}, nil
			}
			return capability.PrincipalFact{}, intent.Failure{
				Code:     "INVALID_TOKEN",
				Category: intent.CategoryAuth,
				Message:  "bad bearer token",
			}
		})),
	)

	if err := intent.Register(engine.Intents(), ItemIntent{}); err != nil {
		t.Fatalf("failed to register intent: %v", err)
	}
	if err := engine.Compile(); err != nil {
		t.Fatalf("failed to compile engine: %v", err)
	}

	makeApp := func() *fh.App {
		a := fh.New()
		a.Post("/items", refhttp.Adapter(engine, "items.create"))
		return a
	}

	// 1. Success case
	app1 := makeApp()
	body := bytes.NewBufferString(`{"name":"widget","count":5}`)
	req, _ := http.NewRequest("POST", "/items", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer valid-user")

	resp, err := app1.Test(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}
	if cc := resp.Header.Get("Cache-Control"); cc != "private, max-age=60" {
		t.Errorf("expected Cache-Control header, got %s", cc)
	}

	respBody, _ := io.ReadAll(resp.Body)
	var out ItemOutput
	if err := json.Unmarshal(respBody, &out); err != nil {
		t.Fatalf("failed to parse JSON response: %v", err)
	}
	if out.SKU != "sku-usr-99-widget" || out.Status != "created" {
		t.Errorf("unexpected output payload: %+v", out)
	}

	// 2. Auth failure case -> 401
	app2 := makeApp()
	body2 := bytes.NewBufferString(`{"name":"widget","count":5}`)
	req2, _ := http.NewRequest("POST", "/items", body2)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("Authorization", "Bearer bad-token")

	resp2, err := app2.Test(req2)
	if err != nil {
		t.Fatalf("request 2 failed: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected status 401, got %d", resp2.StatusCode)
	}

	// 3. Validation failure case -> 422
	app3 := makeApp()
	body3 := bytes.NewBufferString(`{"name":"","count":5}`)
	req3, _ := http.NewRequest("POST", "/items", body3)
	req3.Header.Set("Content-Type", "application/json")
	req3.Header.Set("Authorization", "Bearer valid-user")

	resp3, err := app3.Test(req3)
	if err != nil {
		t.Fatalf("request 3 failed: %v", err)
	}
	defer resp3.Body.Close()

	if resp3.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("expected status 422, got %d", resp3.StatusCode)
	}
}
