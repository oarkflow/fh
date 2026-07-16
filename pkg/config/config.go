// Package config provides small dependency-free production configuration
// helpers for fh applications. It intentionally avoids replacing user config
// systems; it gives deployments a safe baseline with env overrides and duration
// parsing.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type Server struct {
	Addr                string `json:"addr"`
	SecureByDefault     bool   `json:"secure_by_default"`
	ReadTimeout         string `json:"read_timeout"`
	ReadHeaderTimeout   string `json:"read_header_timeout"`
	RequestBodyTimeout  string `json:"request_body_timeout"`
	WriteTimeout        string `json:"write_timeout"`
	HandlerTimeout      string `json:"handler_timeout"`
	IdleTimeout         string `json:"idle_timeout"`
	TLSHandshakeTimeout string `json:"tls_handshake_timeout"`
	HTTP2IdleTimeout    string `json:"http2_idle_timeout"`
	MaxConnections      int    `json:"max_connections"`
	MaxConnectionsPerIP int    `json:"max_connections_per_ip"`
	MaxRequestBodySize  int    `json:"max_request_body_size"`
	MaxHeaderListSize   int    `json:"max_header_list_size"`
	MaxHeaderCount      int    `json:"max_header_count"`
	MaxRequestLineSize  int    `json:"max_request_line_size"`
	DisableHTTP2        bool   `json:"disable_http2"`
	DisableH2C          bool   `json:"disable_h2c"`
	Debug               bool   `json:"debug"`
	Environment         string `json:"environment"`
}

type StartupBanner struct {
	Disabled bool   `json:"disabled"`
	Name     string `json:"name"`
	Version  string `json:"version"`
	Subtitle string `json:"subtitle"`
	Scheme   string `json:"scheme"`
	Address  string `json:"address"`
	ASCIIArt string `json:"ascii_art"`
	Color    bool   `json:"color"`
}

type Reliability struct {
	Enabled     bool   `json:"enabled"`
	DataDir     string `json:"data_dir"`
	QueueDir    string `json:"queue_dir"`
	Workers     int    `json:"workers"`
	MaxAttempts int    `json:"max_attempts"`
}

type Config struct {
	Server        Server        `json:"server"`
	StartupBanner StartupBanner `json:"startup_banner"`
	Reliability   Reliability   `json:"reliability"`
}

func LoadJSON(r io.Reader) (Config, error) {
	var c Config
	err := json.NewDecoder(r).Decode(&c)
	return c, err
}
func LoadJSONFile(path string) (Config, error) {
	cleaned := sanitizePath(path)
	if cleaned == "" {
		return Config{}, fmt.Errorf("config: invalid path %q", path)
	}
	f, err := os.Open(cleaned)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	return LoadJSON(f)
}

// ApplyEnv overlays values from environment variables using the supplied prefix.
// Example with prefix FH: FH_ADDR, FH_READ_TIMEOUT, FH_RELIABILITY_ENABLED.
func ApplyEnv(c Config, prefix string) (Config, error) {
	var errs []error
	p := strings.TrimRight(strings.ToUpper(strings.TrimSpace(prefix)), "_")
	key := func(k string) string {
		if p == "" {
			return k
		}
		return p + "_" + k
	}
	if v := os.Getenv(key("ADDR")); v != "" {
		c.Server.Addr = v
	}
	if v := os.Getenv(key("READ_TIMEOUT")); v != "" {
		c.Server.ReadTimeout = v
	}
	if v := os.Getenv(key("READ_HEADER_TIMEOUT")); v != "" {
		c.Server.ReadHeaderTimeout = v
	}
	if v := os.Getenv(key("REQUEST_BODY_TIMEOUT")); v != "" {
		c.Server.RequestBodyTimeout = v
	}
	if v := os.Getenv(key("WRITE_TIMEOUT")); v != "" {
		c.Server.WriteTimeout = v
	}
	if v := os.Getenv(key("HANDLER_TIMEOUT")); v != "" {
		c.Server.HandlerTimeout = v
	}
	if v := os.Getenv(key("IDLE_TIMEOUT")); v != "" {
		c.Server.IdleTimeout = v
	}
	if v := os.Getenv(key("TLS_HANDSHAKE_TIMEOUT")); v != "" {
		c.Server.TLSHandshakeTimeout = v
	}
	if v := os.Getenv(key("HTTP2_IDLE_TIMEOUT")); v != "" {
		c.Server.HTTP2IdleTimeout = v
	}
	if v := os.Getenv(key("ENVIRONMENT")); v != "" {
		c.Server.Environment = v
	}
	if v := os.Getenv(key("DEBUG")); v != "" {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_DEBUG: %w", err))
		} else {
			c.Server.Debug = b
		}
	}
	if v := os.Getenv(key("SECURE_BY_DEFAULT")); v != "" {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_SECURE_BY_DEFAULT: %w", err))
		} else {
			c.Server.SecureByDefault = b
		}
	}
	if v := os.Getenv(key("DISABLE_H2C")); v != "" {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_DISABLE_H2C: %w", err))
		} else {
			c.Server.DisableH2C = b
		}
	}
	if v := os.Getenv(key("MAX_CONNECTIONS")); v != "" {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_MAX_CONNECTIONS: %w", err))
		} else {
			c.Server.MaxConnections = n
		}
	}
	if v := os.Getenv(key("MAX_CONNECTIONS_PER_IP")); v != "" {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_MAX_CONNECTIONS_PER_IP: %w", err))
		} else {
			c.Server.MaxConnectionsPerIP = n
		}
	}
	if v := os.Getenv(key("MAX_REQUEST_BODY_SIZE")); v != "" {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_MAX_REQUEST_BODY_SIZE: %w", err))
		} else {
			c.Server.MaxRequestBodySize = n
		}
	}
	if v := os.Getenv(key("STARTUP_BANNER_DISABLED")); v != "" {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_STARTUP_BANNER_DISABLED: %w", err))
		} else {
			c.StartupBanner.Disabled = b
		}
	}
	if v := os.Getenv(key("STARTUP_BANNER_NAME")); v != "" {
		c.StartupBanner.Name = v
	}
	if v := os.Getenv(key("STARTUP_BANNER_VERSION")); v != "" {
		c.StartupBanner.Version = v
	}
	if v := os.Getenv(key("STARTUP_BANNER_SUBTITLE")); v != "" {
		c.StartupBanner.Subtitle = v
	}
	if v := os.Getenv(key("STARTUP_BANNER_COLOR")); v != "" {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_STARTUP_BANNER_COLOR: %w", err))
		} else {
			c.StartupBanner.Color = b
		}
	}
	if v := os.Getenv(key("RELIABILITY_ENABLED")); v != "" {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_RELIABILITY_ENABLED: %w", err))
		} else {
			c.Reliability.Enabled = b
		}
	}
	if v := os.Getenv(key("RELIABILITY_DATA_DIR")); v != "" {
		c.Reliability.DataDir = v
	}
	if v := os.Getenv(key("RELIABILITY_QUEUE_DIR")); v != "" {
		c.Reliability.QueueDir = v
	}
	if v := os.Getenv(key("RELIABILITY_WORKERS")); v != "" {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("FH_RELIABILITY_WORKERS: %w", err))
		} else {
			c.Reliability.Workers = n
		}
	}
	if len(errs) > 0 {
		return c, errors.Join(errs...)
	}
	return c, nil
}

func (c Config) Validate() error {
	for name, raw := range map[string]string{
		"read_timeout": c.Server.ReadTimeout, "read_header_timeout": c.Server.ReadHeaderTimeout,
		"request_body_timeout": c.Server.RequestBodyTimeout, "write_timeout": c.Server.WriteTimeout,
		"handler_timeout": c.Server.HandlerTimeout, "idle_timeout": c.Server.IdleTimeout,
		"tls_handshake_timeout": c.Server.TLSHandshakeTimeout, "http2_idle_timeout": c.Server.HTTP2IdleTimeout,
	} {
		if raw != "" {
			if _, err := time.ParseDuration(raw); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	if c.Server.MaxHeaderCount < 0 || c.Server.MaxConnections < 0 || c.Server.MaxConnectionsPerIP < 0 || c.Server.MaxRequestBodySize < 0 {
		return fmt.Errorf("config: numeric limits must be >= 0 (MaxHeaderCount=%d, MaxConnections=%d, MaxConnectionsPerIP=%d, MaxRequestBodySize=%d)", c.Server.MaxHeaderCount, c.Server.MaxConnections, c.Server.MaxConnectionsPerIP, c.Server.MaxRequestBodySize)
	}
	return nil
}

func (c Config) AppConfig() (fh.Config, error) {
	if err := c.Validate(); err != nil {
		return fh.Config{}, err
	}
	var out fh.Config
	var err error
	out.ReadTimeout, err = dur(c.Server.ReadTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("read_timeout: %w", err)
	}
	out.ReadHeaderTimeout, err = dur(c.Server.ReadHeaderTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("read_header_timeout: %w", err)
	}
	out.RequestBodyTimeout, err = dur(c.Server.RequestBodyTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("request_body_timeout: %w", err)
	}
	out.WriteTimeout, err = dur(c.Server.WriteTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("write_timeout: %w", err)
	}
	out.HandlerTimeout, err = dur(c.Server.HandlerTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("handler_timeout: %w", err)
	}
	out.IdleTimeout, err = dur(c.Server.IdleTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("idle_timeout: %w", err)
	}
	out.TLSHandshakeTimeout, err = dur(c.Server.TLSHandshakeTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("tls_handshake_timeout: %w", err)
	}
	out.HTTP2IdleTimeout, err = dur(c.Server.HTTP2IdleTimeout)
	if err != nil {
		return fh.Config{}, fmt.Errorf("http2_idle_timeout: %w", err)
	}
	if out.ReadTimeout == 0 {
		out.ReadTimeout = 30 * time.Second
	}
	if out.WriteTimeout == 0 {
		out.WriteTimeout = 60 * time.Second
	}
	if out.IdleTimeout == 0 {
		out.IdleTimeout = 120 * time.Second
	}
	if out.ReadHeaderTimeout == 0 {
		out.ReadHeaderTimeout = 5 * time.Second
	}
	out.MaxConnections = c.Server.MaxConnections
	out.MaxConnectionsPerIP = c.Server.MaxConnectionsPerIP
	out.SecureByDefault = c.Server.SecureByDefault
	out.MaxRequestBodySize = c.Server.MaxRequestBodySize
	out.MaxHeaderListSize = c.Server.MaxHeaderListSize
	out.MaxHeaderCount = c.Server.MaxHeaderCount
	out.MaxRequestLineSize = c.Server.MaxRequestLineSize
	out.DisableHTTP2 = c.Server.DisableHTTP2
	out.DisableH2C = c.Server.DisableH2C
	out.Debug = c.Server.Debug
	out.StartupBanner = fh.StartupBannerConfig{
		Disabled: c.StartupBanner.Disabled,
		Name:     c.StartupBanner.Name,
		Version:  c.StartupBanner.Version,
		Subtitle: c.StartupBanner.Subtitle,
		Scheme:   c.StartupBanner.Scheme,
		Address:  c.StartupBanner.Address,
		ASCIIArt: c.StartupBanner.ASCIIArt,
		Color:    c.StartupBanner.Color,
	}
	switch strings.ToLower(c.Server.Environment) {
	case "prod", "production":
		out.Environment = fh.EnvProduction
	case "dev", "development":
		out.Environment = fh.EnvDevelopment
	}
	if c.Reliability.Enabled {
		out.Reliability.Enabled = true
		out.Reliability.DataDir = c.Reliability.DataDir
		out.Reliability.QueueDir = c.Reliability.QueueDir
		out.Reliability.QueueWorkers = c.Reliability.Workers
		out.Reliability.QueueMaxAttempts = c.Reliability.MaxAttempts
	}
	return out, nil
}

func NewApp(c Config) (*fh.App, error) {
	ac, err := c.AppConfig()
	if err != nil {
		return nil, err
	}
	return fh.NewWithConfig(ac), nil
}
func dur(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return time.ParseDuration(s)
}
func sanitizePath(p string) string {
	cleaned := filepath.Clean(p)
	if strings.Contains(cleaned, "..") {
		return ""
	}
	return cleaned
}
func parseInt(s string) (int, error)   { return strconv.Atoi(strings.TrimSpace(s)) }
func parseBool(s string) (bool, error) { return strconv.ParseBool(strings.TrimSpace(s)) }
