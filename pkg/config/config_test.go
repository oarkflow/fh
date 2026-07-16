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

func TestSecureByDefaultConfigAndEnv(t *testing.T) {
	cfg, err := LoadJSON(strings.NewReader(`{"server":{"secure_by_default":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	ac, err := cfg.AppConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !ac.SecureByDefault {
		t.Fatal("secure_by_default was not mapped to fh.Config")
	}

	t.Setenv("FH_SECURE_BY_DEFAULT", "true")
	cfg, err = ApplyEnv(Config{}, "FH")
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Server.SecureByDefault {
		t.Fatal("FH_SECURE_BY_DEFAULT was not applied")
	}
}

func TestProtocolTimeoutAndH2CConfig(t *testing.T) {
	cfg, err := LoadJSON(strings.NewReader(`{"server":{"read_header_timeout":"3s","request_body_timeout":"4s","handler_timeout":"5s","tls_handshake_timeout":"6s","http2_idle_timeout":"7s","max_connections_per_ip":25,"disable_h2c":true}}`))
	if err != nil {
		t.Fatal(err)
	}
	ac, err := cfg.AppConfig()
	if err != nil {
		t.Fatal(err)
	}
	if ac.ReadHeaderTimeout.String() != "3s" || ac.RequestBodyTimeout.String() != "4s" || ac.HandlerTimeout.String() != "5s" || ac.TLSHandshakeTimeout.String() != "6s" || ac.HTTP2IdleTimeout.String() != "7s" || ac.MaxConnectionsPerIP != 25 || !ac.DisableH2C {
		t.Fatalf("protocol config was not mapped: %#v", ac)
	}

	t.Setenv("FH_REQUEST_BODY_TIMEOUT", "8s")
	t.Setenv("FH_DISABLE_H2C", "true")
	t.Setenv("FH_MAX_CONNECTIONS_PER_IP", "30")
	cfg, err = ApplyEnv(Config{}, "FH")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.RequestBodyTimeout != "8s" || cfg.Server.MaxConnectionsPerIP != 30 || !cfg.Server.DisableH2C {
		t.Fatalf("protocol env was not applied: %#v", cfg.Server)
	}
}
