package fh_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestHealthChecksAffectReadiness(t *testing.T) {
	app := fh.New()
	ready, checks := app.HealthStatus(context.Background())
	if !ready || len(checks) != 0 {
		t.Fatalf("default readiness = %v %#v", ready, checks)
	}
	app.AddHealthCheck("db", time.Second, func(context.Context) error { return errors.New("down") })
	ready, checks = app.HealthStatus(context.Background())
	if ready || len(checks) != 1 || checks[0].Status != "error" || checks[0].Name != "db" {
		t.Fatalf("unhealthy readiness = %v %#v", ready, checks)
	}
	app.AddHealthCheck("db", time.Second, func(context.Context) error { return nil })
	ready, checks = app.HealthStatus(context.Background())
	if !ready || len(checks) != 1 || checks[0].Status != "ok" {
		t.Fatalf("healthy readiness = %v %#v", ready, checks)
	}
}
