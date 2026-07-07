// Package config provides small dependency-free production configuration
// helpers for fh applications. It intentionally avoids replacing user config
// systems; it gives deployments a safe baseline with env overrides and duration
// parsing.
package config

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

type Server struct {
	Addr               string `json:"addr"`
	ReadTimeout        string `json:"read_timeout"`
	WriteTimeout       string `json:"write_timeout"`
	IdleTimeout        string `json:"idle_timeout"`
	MaxConnections     int    `json:"max_connections"`
	MaxRequestBodySize int    `json:"max_request_body_size"`
	MaxHeaderListSize  int    `json:"max_header_list_size"`
	MaxHeaderCount     int    `json:"max_header_count"`
	MaxRequestLineSize int    `json:"max_request_line_size"`
	DisableHTTP2       bool   `json:"disable_http2"`
	Debug              bool   `json:"debug"`
	Environment        string `json:"environment"`
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
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	return LoadJSON(f)
}

// ApplyEnv overlays values from environment variables using the supplied prefix.
// Example with prefix FH: FH_ADDR, FH_READ_TIMEOUT, FH_RELIABILITY_ENABLED.
func ApplyEnv(c Config, prefix string) Config {
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
	if v := os.Getenv(key("WRITE_TIMEOUT")); v != "" {
		c.Server.WriteTimeout = v
	}
	if v := os.Getenv(key("IDLE_TIMEOUT")); v != "" {
		c.Server.IdleTimeout = v
	}
	if v := os.Getenv(key("ENVIRONMENT")); v != "" {
		c.Server.Environment = v
	}
	if v := os.Getenv(key("DEBUG")); v != "" {
		c.Server.Debug = parseBool(v)
	}
	if v := os.Getenv(key("MAX_CONNECTIONS")); v != "" {
		c.Server.MaxConnections = parseInt(v)
	}
	if v := os.Getenv(key("MAX_REQUEST_BODY_SIZE")); v != "" {
		c.Server.MaxRequestBodySize = parseInt(v)
	}
	if v := os.Getenv(key("STARTUP_BANNER_DISABLED")); v != "" {
		c.StartupBanner.Disabled = parseBool(v)
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
		c.StartupBanner.Color = parseBool(v)
	}
	if v := os.Getenv(key("RELIABILITY_ENABLED")); v != "" {
		c.Reliability.Enabled = parseBool(v)
	}
	if v := os.Getenv(key("RELIABILITY_DATA_DIR")); v != "" {
		c.Reliability.DataDir = v
	}
	if v := os.Getenv(key("RELIABILITY_QUEUE_DIR")); v != "" {
		c.Reliability.QueueDir = v
	}
	if v := os.Getenv(key("RELIABILITY_WORKERS")); v != "" {
		c.Reliability.Workers = parseInt(v)
	}
	return c
}

func (c Config) Validate() error {
	for name, raw := range map[string]string{"read_timeout": c.Server.ReadTimeout, "write_timeout": c.Server.WriteTimeout, "idle_timeout": c.Server.IdleTimeout} {
		if raw != "" {
			if _, err := time.ParseDuration(raw); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
	}
	if c.Server.MaxHeaderCount < 0 || c.Server.MaxConnections < 0 || c.Server.MaxRequestBodySize < 0 {
		return fmt.Errorf("numeric limits must be >= 0")
	}
	return nil
}

func (c Config) AppConfig() (fh.Config, error) {
	if err := c.Validate(); err != nil {
		return fh.Config{}, err
	}
	var out fh.Config
	out.ReadTimeout = dur(c.Server.ReadTimeout)
	out.WriteTimeout = dur(c.Server.WriteTimeout)
	out.IdleTimeout = dur(c.Server.IdleTimeout)
	out.MaxConnections = c.Server.MaxConnections
	out.MaxRequestBodySize = c.Server.MaxRequestBodySize
	out.MaxHeaderListSize = c.Server.MaxHeaderListSize
	out.MaxHeaderCount = c.Server.MaxHeaderCount
	out.MaxRequestLineSize = c.Server.MaxRequestLineSize
	out.DisableHTTP2 = c.Server.DisableHTTP2
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
func dur(s string) time.Duration {
	if s == "" {
		return 0
	}
	d, _ := time.ParseDuration(s)
	return d
}
func parseInt(s string) int   { n, _ := strconv.Atoi(strings.TrimSpace(s)); return n }
func parseBool(s string) bool { v, _ := strconv.ParseBool(strings.TrimSpace(s)); return v }
