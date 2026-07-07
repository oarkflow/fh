package config

import (
	"strings"
	"testing"
)

func TestLoadJSONAppConfig(t *testing.T) {
	cfg, err := LoadJSON(strings.NewReader(`{"server":{"read_timeout":"2s","environment":"production"},"reliability":{"enabled":true,"workers":2}}`))
	if err != nil {
		t.Fatal(err)
	}
	ac, err := cfg.AppConfig()
	if err != nil {
		t.Fatal(err)
	}
	if ac.ReadTimeout.String() != "2s" {
		t.Fatalf("timeout=%v", ac.ReadTimeout)
	}
	if !ac.Reliability.Enabled || ac.Reliability.QueueWorkers != 2 {
		t.Fatalf("bad reliability %+v", ac.Reliability)
	}
}
