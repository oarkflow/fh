package fh

import (
	"testing"
	"time"
)

func TestNewWithConfigUsesDefaultsForOmittedFields(t *testing.T) {
	app := NewWithConfig(Config{})

	if app.cfg.ReadBufferSize != defaultConfig.ReadBufferSize {
		t.Fatalf("ReadBufferSize = %d, want %d", app.cfg.ReadBufferSize, defaultConfig.ReadBufferSize)
	}
	if app.cfg.MaxRequestBodySize != defaultConfig.MaxRequestBodySize {
		t.Fatalf("MaxRequestBodySize = %d, want %d", app.cfg.MaxRequestBodySize, defaultConfig.MaxRequestBodySize)
	}
	if app.cfg.MaxHeaderListSize != defaultConfig.MaxHeaderListSize {
		t.Fatalf("MaxHeaderListSize = %d, want %d", app.cfg.MaxHeaderListSize, defaultConfig.MaxHeaderListSize)
	}
	if app.cfg.MaxHeaderCount != defaultConfig.MaxHeaderCount {
		t.Fatalf("MaxHeaderCount = %d, want %d", app.cfg.MaxHeaderCount, defaultConfig.MaxHeaderCount)
	}
	if app.cfg.MaxRequestLineSize != defaultConfig.MaxRequestLineSize {
		t.Fatalf("MaxRequestLineSize = %d, want %d", app.cfg.MaxRequestLineSize, defaultConfig.MaxRequestLineSize)
	}
	if app.cfg.MaxConcurrentStreams != defaultConfig.MaxConcurrentStreams {
		t.Fatalf("MaxConcurrentStreams = %d, want %d", app.cfg.MaxConcurrentStreams, defaultConfig.MaxConcurrentStreams)
	}
	if app.cfg.Environment != defaultConfig.Environment {
		t.Fatalf("Environment = %q, want %q", app.cfg.Environment, defaultConfig.Environment)
	}
}

func TestNewWithConfigPreservesOverrides(t *testing.T) {
	cfg := Config{
		ReadBufferSize:       32 << 10,
		MaxRequestBodySize:   8 << 20,
		MaxHeaderListSize:    24 << 10,
		MaxHeaderCount:       48,
		MaxRequestLineSize:   4 << 10,
		MaxConcurrentStreams: 64,
		Environment:          EnvDevelopment,
	}
	app := NewWithConfig(cfg)

	if app.cfg.ReadBufferSize != cfg.ReadBufferSize ||
		app.cfg.MaxRequestBodySize != cfg.MaxRequestBodySize ||
		app.cfg.MaxHeaderListSize != cfg.MaxHeaderListSize ||
		app.cfg.MaxHeaderCount != cfg.MaxHeaderCount ||
		app.cfg.MaxRequestLineSize != cfg.MaxRequestLineSize ||
		app.cfg.MaxConcurrentStreams != cfg.MaxConcurrentStreams ||
		app.cfg.Environment != cfg.Environment {
		t.Fatalf("explicit config was not preserved: %#v", app.cfg)
	}
}

func TestSecureByDefaultResolvesFailClosedBaseline(t *testing.T) {
	app := NewWithConfig(Config{
		SecureByDefault:             true,
		Mode:                        ModeFast,
		Environment:                 EnvDevelopment,
		Debug:                       true,
		ErrorOptions:                ErrorOptions{Environment: EnvDevelopment, ExposeDebug: true, ExposeStackTrace: true, ExposeCauses: true},
		DisablePanicRecovery:        true,
		StrictHeaderValueValidation: false,
		ServerHeader:                "leaky-version",
		ReadHeaderTimeout:           time.Minute,
		ReadTimeout:                 time.Minute,
		WriteTimeout:                time.Minute,
		IdleTimeout:                 time.Hour,
		ReadBufferSize:              1 << 20,
		MaxConnections:              100_000,
		MaxRequestBodySize:          64 << 20,
		MaxHeaderListSize:           1 << 20,
		MaxHeaderCount:              1000,
		MaxRequestLineSize:          64 << 10,
		MaxConcurrentStreams:        1000,
	})

	if app.cfg.Mode != ModeStrict || app.cfg.Debug || app.cfg.DisablePanicRecovery || !app.cfg.StrictHeaderValueValidation {
		t.Fatalf("secure baseline was weakened: %#v", app.cfg)
	}
	if app.cfg.Environment != EnvProduction || app.cfg.ErrorOptions.Environment != EnvProduction || app.cfg.ErrorOptions.ExposeDebug || app.cfg.ErrorOptions.ExposeStackTrace || app.cfg.ErrorOptions.ExposeCauses {
		t.Fatalf("debug error exposure was not disabled: %#v", app.cfg.ErrorOptions)
	}
	if app.cfg.ServerHeader != "" {
		t.Fatalf("ServerHeader = %q, want empty", app.cfg.ServerHeader)
	}
	if !app.cfg.DisableH2C {
		t.Fatal("secure baseline did not disable cleartext HTTP/2")
	}
	if app.cfg.ReadHeaderTimeout != 5*time.Second || app.cfg.ReadTimeout != 10*time.Second || app.cfg.WriteTimeout != 30*time.Second || app.cfg.IdleTimeout != 60*time.Second {
		t.Fatalf("timeouts were not bounded: %#v", app.cfg)
	}
	if app.cfg.ReadBufferSize != 16<<10 || app.cfg.MaxConnections != 10_000 {
		t.Fatalf("connection resources were not bounded: %#v", app.cfg)
	}
	if app.cfg.MaxRequestBodySize != 4<<20 || app.cfg.MaxHeaderListSize != 32<<10 || app.cfg.MaxHeaderCount != 64 || app.cfg.MaxRequestLineSize != 8<<10 || app.cfg.MaxConcurrentStreams != 128 {
		t.Fatalf("input limits were not bounded: %#v", app.cfg)
	}
	if !app.cfg.Redaction.Enabled {
		t.Fatal("secure baseline did not enable redaction")
	}
}

func TestSecureByDefaultPreservesStricterLimits(t *testing.T) {
	app := NewWithConfig(Config{
		SecureByDefault:    true,
		MaxRequestBodySize: 1024,
		MaxHeaderListSize:  2048,
		MaxHeaderCount:     12,
		MaxRequestLineSize: 1024,
		ReadBufferSize:     4096,
		MaxConnections:     100,
		ReadTimeout:        time.Second,
	})
	if app.cfg.MaxRequestBodySize != 1024 || app.cfg.MaxHeaderListSize != 2048 || app.cfg.MaxHeaderCount != 12 || app.cfg.MaxRequestLineSize != 1024 || app.cfg.ReadBufferSize != 4096 || app.cfg.MaxConnections != 100 || app.cfg.ReadTimeout != time.Second {
		t.Fatalf("stricter caller limits were changed: %#v", app.cfg)
	}
}
