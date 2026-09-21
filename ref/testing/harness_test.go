package testing_test

import (
	"context"
	"testing"

	"github.com/oarkflow/fh/ref/fact"
	"github.com/oarkflow/fh/ref/runtime"
	reftest "github.com/oarkflow/fh/ref/testing"
)

type SampleFact struct {
	Role string
}

func TestTestingHarness(t *testing.T) {
	key := fact.NewKey[SampleFact]("sample.fact")

	tb := reftest.NewTest().
		WithInput([]byte("payload"), "text/plain").
		WithIntent("sample.intent").
		WithPrincipal("secret-token", "")

	reftest.Given(tb, key, SampleFact{Role: "manager"})

	nc := tb.BuildNodeContext(context.Background(), 5)
	reftest.AssertFact(t, nc, key, SampleFact{Role: "manager"})

	res := &runtime.DispatchResult{
		Value: "success",
	}
	reftest.AssertOutcome(t, res, "success")
}
