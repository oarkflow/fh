// Package config provides small dependency-free production configuration
// helpers for fh applications. It intentionally avoids replacing user config
// systems; it gives deployments a safe baseline with env overrides and duration
// parsing.
package config

import (
	"bytes"
	"encoding/json"
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
	Addr                     string   `json:"addr"`
	SecureByDefault          bool     `json:"secure_by_default"`
	AllowedHosts             []string `json:"allowed_hosts"`
	HSTSPreload              bool     `json:"hsts_preload"`
	ContentSecurityPolicy    string   `json:"content_security_policy"`
	ReadTimeout              string   `json:"read_timeout"`
	ReadHeaderTimeout        string   `json:"read_header_timeout"`
	RequestBodyTimeout       string   `json:"request_body_timeout"`
	WriteTimeout             string   `json:"write_timeout"`
	HandlerTimeout           string   `json:"handler_timeout"`
	IdleTimeout              string   `json:"idle_timeout"`
	TLSHandshakeTimeout      string   `json:"tls_handshake_timeout"`
	HTTP2IdleTimeout         string   `json:"http2_idle_timeout"`
	MaxConnections           int      `json:"max_connections"`
	MaxConnectionsPerIP      int      `json:"max_connections_per_ip"`
	ReadBufferSize           int      `json:"read_buffer_size"`
	MaxRequestBodySize       int      `json:"max_request_body_size"`
	MaxInFlightRequests      int64    `json:"max_in_flight_requests"`
	MaxGoroutines            int      `json:"max_goroutines"`
	MaxHeapBytes             string   `json:"max_heap_bytes"`
	ResourceCheckInterval    string   `json:"resource_check_interval"`
	MaxBufferedRequestBytes  int64    `json:"max_buffered_request_bytes"`
	MaxBufferedResponseBytes int64    `json:"max_buffered_response_bytes"`
	MaxHeaderListSize        int      `json:"max_header_list_size"`
	MaxHeaderCount           int      `json:"max_header_count"`
	MaxRequestLineSize       int      `json:"max_request_line_size"`
	MaxConcurrentStreams     uint32   `json:"max_concurrent_streams"`
	DisableHTTP2             bool     `json:"disable_http2"`
	DisableH2C               bool     `json:"disable_h2c"`
	Debug                    bool     `json:"debug"`
	Environment              string   `json:"environment"`
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
	// Prefix is used by ApplyEnv/LoadEnvFile when their prefix argument is empty.
	Prefix        string        `json:"prefix,omitempty"`
	Server        Server        `json:"server"`
	StartupBanner StartupBanner `json:"startup_banner"`
	Reliability   Reliability   `json:"reliability"`
}

const maxConfigInputBytes = 1 << 20

func LoadJSON(r io.Reader) (Config, error) {
	var c Config
	if r == nil {
		return c, fmt.Errorf("config: nil JSON reader")
	}
	data, err := io.ReadAll(io.LimitReader(r, maxConfigInputBytes+1))
	if err != nil {
		return c, fmt.Errorf("config: read JSON: %w", err)
	}
	if len(data) > maxConfigInputBytes {
		return c, fmt.Errorf("config: JSON input exceeds %d bytes", maxConfigInputBytes)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return c, fmt.Errorf("config: JSON root must be an object")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: decode JSON: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("config: multiple JSON values are not allowed")
		}
		return Config{}, fmt.Errorf("config: trailing JSON data: %w", err)
	}
	return c, nil
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

func (c Config) Validate() error {
	if _, err := normalizePrefix(c.Prefix); err != nil {
		return err
	}
	for _, field := range []struct{ name, value string }{
		{"read_timeout", c.Server.ReadTimeout},
		{"read_header_timeout", c.Server.ReadHeaderTimeout},
		{"request_body_timeout", c.Server.RequestBodyTimeout},
		{"write_timeout", c.Server.WriteTimeout},
		{"handler_timeout", c.Server.HandlerTimeout},
		{"idle_timeout", c.Server.IdleTimeout},
		{"tls_handshake_timeout", c.Server.TLSHandshakeTimeout},
		{"http2_idle_timeout", c.Server.HTTP2IdleTimeout},
		{"resource_check_interval", c.Server.ResourceCheckInterval},
	} {
		if field.value != "" {
			duration, err := time.ParseDuration(field.value)
			if err != nil {
				return fmt.Errorf("%s: %w", field.name, err)
			}
			if duration < 0 {
				return fmt.Errorf("%s: duration must be >= 0", field.name)
			}
		}
	}
	for _, field := range []struct {
		name  string
		value int64
	}{
		{"max_connections", int64(c.Server.MaxConnections)},
		{"max_connections_per_ip", int64(c.Server.MaxConnectionsPerIP)},
		{"read_buffer_size", int64(c.Server.ReadBufferSize)},
		{"max_request_body_size", int64(c.Server.MaxRequestBodySize)},
		{"max_in_flight_requests", c.Server.MaxInFlightRequests},
		{"max_goroutines", int64(c.Server.MaxGoroutines)},
		{"max_buffered_request_bytes", c.Server.MaxBufferedRequestBytes},
		{"max_buffered_response_bytes", c.Server.MaxBufferedResponseBytes},
		{"max_header_list_size", int64(c.Server.MaxHeaderListSize)},
		{"max_header_count", int64(c.Server.MaxHeaderCount)},
		{"max_request_line_size", int64(c.Server.MaxRequestLineSize)},
	} {
		if field.value < 0 {
			return fmt.Errorf("%s: value must be >= 0", field.name)
		}
	}
	if c.Server.MaxHeapBytes != "" {
		if _, err := parseBytes(c.Server.MaxHeapBytes); err != nil {
			return fmt.Errorf("max_heap_bytes: %w", err)
		}
	}
	switch strings.ToLower(strings.TrimSpace(c.Server.Environment)) {
	case "", "prod", "production", "dev", "development":
	default:
		return fmt.Errorf("config: environment must be production or development")
	}
	for _, host := range c.Server.AllowedHosts {
		if strings.TrimSpace(host) == "" || host != strings.TrimSpace(host) || len(host) > 512 || strings.ContainsAny(host, " \t/\\@?#") || hasInvalidHeaderControl(host) {
			return fmt.Errorf("config: invalid allowed host %q", host)
		}
	}
	if len(c.Server.ContentSecurityPolicy) > 16<<10 || hasInvalidHeaderControl(c.Server.ContentSecurityPolicy) {
		return fmt.Errorf("config: content security policy is invalid or exceeds 16 KiB")
	}
	if c.Server.HSTSPreload && !c.Server.SecureByDefault {
		return fmt.Errorf("config: hsts_preload requires secure_by_default")
	}
	if c.Reliability.Workers < 0 || c.Reliability.MaxAttempts < 0 {
		return fmt.Errorf("config: reliability workers and max attempts must be >= 0")
	}
	return nil
}

func (c Config) AppConfig() (fh.Config, error) {
	if err := c.Validate(); err != nil {
		return fh.Config{}, err
	}
	var out fh.Config
	var err error
	for _, field := range []struct {
		name   string
		value  string
		target *time.Duration
	}{
		{"read_timeout", c.Server.ReadTimeout, &out.ReadTimeout},
		{"read_header_timeout", c.Server.ReadHeaderTimeout, &out.ReadHeaderTimeout},
		{"request_body_timeout", c.Server.RequestBodyTimeout, &out.RequestBodyTimeout},
		{"write_timeout", c.Server.WriteTimeout, &out.WriteTimeout},
		{"handler_timeout", c.Server.HandlerTimeout, &out.HandlerTimeout},
		{"idle_timeout", c.Server.IdleTimeout, &out.IdleTimeout},
		{"tls_handshake_timeout", c.Server.TLSHandshakeTimeout, &out.TLSHandshakeTimeout},
		{"http2_idle_timeout", c.Server.HTTP2IdleTimeout, &out.HTTP2IdleTimeout},
		{"resource_check_interval", c.Server.ResourceCheckInterval, &out.ResourceCheckInterval},
	} {
		*field.target, err = parseDuration(field.name, field.value)
		if err != nil {
			return fh.Config{}, err
		}
	}
	if out.ReadTimeout == 0 && !c.Server.SecureByDefault {
		out.ReadTimeout = 30 * time.Second
	}
	if out.WriteTimeout == 0 && !c.Server.SecureByDefault {
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
	out.AllowedHosts = append([]string(nil), c.Server.AllowedHosts...)
	out.HSTSPreload = c.Server.HSTSPreload
	out.ContentSecurityPolicy = c.Server.ContentSecurityPolicy
	out.MaxRequestBodySize = c.Server.MaxRequestBodySize
	out.MaxInFlightRequests = c.Server.MaxInFlightRequests
	out.MaxGoroutines = c.Server.MaxGoroutines
	if c.Server.MaxHeapBytes != "" {
		out.MaxHeapBytes, err = parseBytes(c.Server.MaxHeapBytes)
		if err != nil {
			return fh.Config{}, fmt.Errorf("max_heap_bytes: %w", err)
		}
	}
	out.MaxBufferedRequestBytes = c.Server.MaxBufferedRequestBytes
	out.MaxBufferedResponseBytes = c.Server.MaxBufferedResponseBytes
	out.MaxHeaderListSize = c.Server.MaxHeaderListSize
	out.MaxHeaderCount = c.Server.MaxHeaderCount
	out.MaxRequestLineSize = c.Server.MaxRequestLineSize
	out.ReadBufferSize = c.Server.ReadBufferSize
	out.MaxConcurrentStreams = c.Server.MaxConcurrentStreams
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
	switch strings.ToLower(strings.TrimSpace(c.Server.Environment)) {
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
func parseDuration(name, value string) (time.Duration, error) {
	if value == "" {
		return 0, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return duration, nil
}
func sanitizePath(p string) string {
	if strings.TrimSpace(p) == "" {
		return ""
	}
	cleaned := filepath.Clean(p)
	return cleaned
}
func parseInt(s string) (int, error)   { return strconv.Atoi(strings.TrimSpace(s)) }
func parseBool(s string) (bool, error) { return strconv.ParseBool(strings.TrimSpace(s)) }
func parseBytes(s string) (uint64, error) {
	x := strings.TrimSpace(strings.ToLower(s))
	mult := uint64(1)
	for _, unit := range []struct {
		suffix     string
		multiplier uint64
	}{{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30}, {"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30}, {"b", 1}} {
		if strings.HasSuffix(x, unit.suffix) {
			mult = unit.multiplier
			x = strings.TrimSpace(strings.TrimSuffix(x, unit.suffix))
			break
		}
	}
	n, err := strconv.ParseUint(x, 10, 64)
	if err != nil {
		return 0, err
	}
	if n > ^uint64(0)/mult {
		return 0, fmt.Errorf("byte value overflows uint64")
	}
	return n * mult, nil
}

func hasInvalidHeaderControl(value string) bool {
	for i := 0; i < len(value); i++ {
		b := value[i]
		if b < 0x20 && b != '\t' || b == 0x7f {
			return true
		}
	}
	return false
}
