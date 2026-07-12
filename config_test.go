package fh

import "testing"

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
