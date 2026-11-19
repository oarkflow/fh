// Package config provides startup configuration from process environment and dotenv files.
package config

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

// Environment key suffixes and prefix settings supported by this package.
// ApplyEnv and LoadEnvFile prepend one prefix to every setting key.
const (
	DefaultEnvPrefix               = "FH"
	EnvKeyPrefix                   = "ENV_PREFIX"
	EnvKeyAddr                     = "ADDR"
	EnvKeyAllowedHosts             = "ALLOWED_HOSTS"
	EnvKeyContentSecurityPolicy    = "CONTENT_SECURITY_POLICY"
	EnvKeyDebug                    = "DEBUG"
	EnvKeyDisableH2C               = "DISABLE_H2C"
	EnvKeyDisableHTTP2             = "DISABLE_HTTP2"
	EnvKeyEnvironment              = "ENVIRONMENT"
	EnvKeyHandlerTimeout           = "HANDLER_TIMEOUT"
	EnvKeyHSTSPreload              = "HSTS_PRELOAD"
	EnvKeyHTTP2IdleTimeout         = "HTTP2_IDLE_TIMEOUT"
	EnvKeyIdleTimeout              = "IDLE_TIMEOUT"
	EnvKeyMaxBufferedRequestBytes  = "MAX_BUFFERED_REQUEST_BYTES"
	EnvKeyMaxBufferedResponseBytes = "MAX_BUFFERED_RESPONSE_BYTES"
	EnvKeyMaxConcurrentStreams     = "MAX_CONCURRENT_STREAMS"
	EnvKeyMaxConnections           = "MAX_CONNECTIONS"
	EnvKeyMaxConnectionsPerIP      = "MAX_CONNECTIONS_PER_IP"
	EnvKeyMaxGoroutines            = "MAX_GOROUTINES"
	EnvKeyMaxHeaderCount           = "MAX_HEADER_COUNT"
	EnvKeyMaxHeaderListSize        = "MAX_HEADER_LIST_SIZE"
	EnvKeyMaxHeapBytes             = "MAX_HEAP_BYTES"
	EnvKeyMaxInFlightRequests      = "MAX_IN_FLIGHT_REQUESTS"
	EnvKeyMaxRequestBodySize       = "MAX_REQUEST_BODY_SIZE"
	EnvKeyMaxRequestLineSize       = "MAX_REQUEST_LINE_SIZE"
	EnvKeyReadBufferSize           = "READ_BUFFER_SIZE"
	EnvKeyReadHeaderTimeout        = "READ_HEADER_TIMEOUT"
	EnvKeyReadTimeout              = "READ_TIMEOUT"
	EnvKeyReliabilityDataDir       = "RELIABILITY_DATA_DIR"
	EnvKeyReliabilityEnabled       = "RELIABILITY_ENABLED"
	EnvKeyReliabilityMaxAttempts   = "RELIABILITY_MAX_ATTEMPTS"
	EnvKeyReliabilityQueueDir      = "RELIABILITY_QUEUE_DIR"
	EnvKeyReliabilityWorkers       = "RELIABILITY_WORKERS"
	EnvKeyRequestBodyTimeout       = "REQUEST_BODY_TIMEOUT"
	EnvKeyResourceCheckInterval    = "RESOURCE_CHECK_INTERVAL"
	EnvKeySecureByDefault          = "SECURE_BY_DEFAULT"
	EnvKeyStartupBannerAddress     = "STARTUP_BANNER_ADDRESS"
	EnvKeyStartupBannerASCIIArt    = "STARTUP_BANNER_ASCII_ART"
	EnvKeyStartupBannerColor       = "STARTUP_BANNER_COLOR"
	EnvKeyStartupBannerDisabled    = "STARTUP_BANNER_DISABLED"
	EnvKeyStartupBannerName        = "STARTUP_BANNER_NAME"
	EnvKeyStartupBannerScheme      = "STARTUP_BANNER_SCHEME"
	EnvKeyStartupBannerSubtitle    = "STARTUP_BANNER_SUBTITLE"
	EnvKeyStartupBannerVersion     = "STARTUP_BANNER_VERSION"
	EnvKeyTLSHandshakeTimeout      = "TLS_HANDSHAKE_TIMEOUT"
	EnvKeyWriteTimeout             = "WRITE_TIMEOUT"
)

// EnvKeys remaps canonical environment suffixes to application-specific
// suffixes. Unspecified entries retain their EnvKey* default. Values are
// copied and validated for each load, so callers may reuse and later edit a
// map without changing an already-running app's configuration.
type EnvKeys map[string]string

var canonicalEnvKeys = []string{
	EnvKeyAddr, EnvKeyAllowedHosts, EnvKeyContentSecurityPolicy, EnvKeyDebug,
	EnvKeyDisableH2C, EnvKeyDisableHTTP2, EnvKeyEnvironment, EnvKeyHandlerTimeout,
	EnvKeyHSTSPreload, EnvKeyHTTP2IdleTimeout, EnvKeyIdleTimeout,
	EnvKeyMaxBufferedRequestBytes, EnvKeyMaxBufferedResponseBytes,
	EnvKeyMaxConcurrentStreams, EnvKeyMaxConnections, EnvKeyMaxConnectionsPerIP,
	EnvKeyMaxGoroutines, EnvKeyMaxHeaderCount, EnvKeyMaxHeaderListSize,
	EnvKeyMaxHeapBytes, EnvKeyMaxInFlightRequests, EnvKeyMaxRequestBodySize,
	EnvKeyMaxRequestLineSize, EnvKeyReadBufferSize, EnvKeyReadHeaderTimeout,
	EnvKeyReadTimeout, EnvKeyReliabilityDataDir, EnvKeyReliabilityEnabled,
	EnvKeyReliabilityMaxAttempts, EnvKeyReliabilityQueueDir, EnvKeyReliabilityWorkers,
	EnvKeyRequestBodyTimeout, EnvKeyResourceCheckInterval, EnvKeySecureByDefault,
	EnvKeyStartupBannerAddress, EnvKeyStartupBannerASCIIArt, EnvKeyStartupBannerColor,
	EnvKeyStartupBannerDisabled, EnvKeyStartupBannerName, EnvKeyStartupBannerScheme,
	EnvKeyStartupBannerSubtitle, EnvKeyStartupBannerVersion,
	EnvKeyTLSHandshakeTimeout, EnvKeyWriteTimeout,
}

func resolveEnvKeys(keys EnvKeys) (EnvKeys, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	known := make(map[string]struct{}, len(canonicalEnvKeys))
	resolved := make(EnvKeys, len(canonicalEnvKeys))
	for _, key := range canonicalEnvKeys {
		known[key] = struct{}{}
		resolved[key] = key
	}
	for canonical, replacement := range keys {
		if _, ok := known[canonical]; !ok {
			return nil, fmt.Errorf("config: unknown environment key %q", canonical)
		}
		if !validEnvName(replacement) {
			return nil, fmt.Errorf("config: invalid environment key suffix %q", replacement)
		}
		resolved[canonical] = replacement
	}
	used := make(map[string]string, len(resolved))
	for canonical, suffix := range resolved {
		collisionKey := strings.ToUpper(suffix)
		if previous, exists := used[collisionKey]; exists {
			return nil, fmt.Errorf("config: environment key suffix %q is shared by %s and %s", suffix, previous, canonical)
		}
		used[collisionKey] = canonical
	}
	return resolved, nil
}

// ApplyEnv overlays values from environment variables using the optional
// prefix. With no prefix, it uses FH_ENV_PREFIX and then Config.Prefix.
// Example with prefix FH: FH_ADDR, FH_READ_TIMEOUT, FH_RELIABILITY_ENABLED.
func ApplyEnv(c Config, prefixes ...string) (Config, error) {
	prefix, err := prefixArgument(prefixes)
	if err != nil {
		return c, err
	}
	return ApplyEnvWithKeys(c, prefix, nil)
}

// ApplyEnvWithKeys applies process environment variables using optional key
// suffix overrides. For example, EnvKeys{EnvKeyMaxHeapBytes: "HEAP_LIMIT"}
// reads FH_HEAP_LIMIT when prefix is "FH".
func ApplyEnvWithKeys(c Config, prefix string, keys EnvKeys) (Config, error) {
	resolved, err := resolveEnvKeys(keys)
	if err != nil {
		return c, err
	}
	return applyEnv(c, prefix, resolved, os.LookupEnv)
}

// LoadEnvFile overlays a dotenv file, then process environment variables (which
// take precedence). Values are read at startup; changing the file takes effect
// when the process is restarted. It does not mutate the process environment.
func LoadEnvFile(path string, c Config, prefixes ...string) (Config, error) {
	prefix, err := prefixArgument(prefixes)
	if err != nil {
		return c, err
	}
	return LoadEnvFileWithKeys(path, c, prefix, nil)
}

func prefixArgument(prefixes []string) (string, error) {
	if len(prefixes) > 1 {
		return "", fmt.Errorf("config: at most one environment prefix may be supplied")
	}
	if len(prefixes) == 0 {
		return "", nil
	}
	return prefixes[0], nil
}

// LoadEnvFileWithKeys is LoadEnvFile with optional environment key suffix
// overrides. Process environment values still take precedence over file values.
func LoadEnvFileWithKeys(path string, c Config, prefix string, keys EnvKeys) (Config, error) {
	resolved, err := resolveEnvKeys(keys)
	if err != nil {
		return c, err
	}
	cleaned := sanitizePath(path)
	if cleaned == "" {
		return c, fmt.Errorf("config: dotenv path is empty")
	}
	f, err := os.Open(cleaned)
	if err != nil {
		return c, err
	}
	defer f.Close()
	contents, err := io.ReadAll(io.LimitReader(f, maxConfigInputBytes+1))
	if err != nil {
		return c, fmt.Errorf("config: read dotenv file: %w", err)
	}
	if len(contents) > maxConfigInputBytes {
		return c, fmt.Errorf("config: dotenv input exceeds %d bytes", maxConfigInputBytes)
	}
	values := make(map[string]string)
	s := bufio.NewScanner(bytes.NewReader(contents))
	s.Buffer(make([]byte, 4096), 64<<10)
	for line := 1; s.Scan(); line++ {
		raw := strings.TrimSpace(s.Text())
		if raw == "" || strings.HasPrefix(raw, "#") {
			continue
		}
		if strings.HasPrefix(raw, "export ") || strings.HasPrefix(raw, "export\t") {
			raw = strings.TrimSpace(raw[len("export"):])
		}
		k, v, ok := strings.Cut(raw, "=")
		k = strings.TrimSpace(k)
		if !ok || !validEnvName(k) {
			return c, fmt.Errorf("config: invalid dotenv entry at line %d", line)
		}
		v, err = parseDotEnvValue(strings.TrimSpace(v))
		if err != nil {
			return c, fmt.Errorf("config: dotenv line %d: %w", line, err)
		}
		values[k] = v
	}
	if err := s.Err(); err != nil {
		return c, err
	}
	return applyEnv(c, prefix, resolved, func(k string) (string, bool) {
		if v, ok := os.LookupEnv(k); ok {
			return v, true
		}
		v, ok := values[k]
		return v, ok
	})
}

func applyEnv(c Config, prefix string, keys EnvKeys, getenv func(string) (string, bool)) (Config, error) {
	original := c
	var errs []error
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		if configured, ok := getenv(DefaultEnvPrefix + "_" + EnvKeyPrefix); ok {
			prefix = configured
		} else if c.Prefix != "" {
			prefix = c.Prefix
		} else {
			prefix = DefaultEnvPrefix
		}
	}
	p, err := normalizePrefix(prefix)
	if err != nil {
		return c, err
	}
	key := func(k string) string {
		if replacement, ok := keys[k]; ok {
			k = replacement
		}
		if p == "" {
			return k
		}
		return p + "_" + k
	}
	if v, ok := getenv(key(EnvKeyAddr)); ok {
		c.Server.Addr = v
	}
	if v, ok := getenv(key(EnvKeyReadTimeout)); ok {
		c.Server.ReadTimeout = v
	}
	if v, ok := getenv(key(EnvKeyReadHeaderTimeout)); ok {
		c.Server.ReadHeaderTimeout = v
	}
	if v, ok := getenv(key(EnvKeyRequestBodyTimeout)); ok {
		c.Server.RequestBodyTimeout = v
	}
	if v, ok := getenv(key(EnvKeyWriteTimeout)); ok {
		c.Server.WriteTimeout = v
	}
	if v, ok := getenv(key(EnvKeyHandlerTimeout)); ok {
		c.Server.HandlerTimeout = v
	}
	if v, ok := getenv(key(EnvKeyIdleTimeout)); ok {
		c.Server.IdleTimeout = v
	}
	if v, ok := getenv(key(EnvKeyTLSHandshakeTimeout)); ok {
		c.Server.TLSHandshakeTimeout = v
	}
	if v, ok := getenv(key(EnvKeyHTTP2IdleTimeout)); ok {
		c.Server.HTTP2IdleTimeout = v
	}
	if v, ok := getenv(key(EnvKeyEnvironment)); ok {
		c.Server.Environment = v
	}
	if v, ok := getenv(key(EnvKeyDebug)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyDebug), err))
		} else {
			c.Server.Debug = b
		}
	}
	if v, ok := getenv(key(EnvKeySecureByDefault)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeySecureByDefault), err))
		} else {
			c.Server.SecureByDefault = b
		}
	}
	if v, ok := getenv(key(EnvKeyAllowedHosts)); ok {
		hosts := strings.Split(v, ",")
		for i := range hosts {
			hosts[i] = strings.TrimSpace(hosts[i])
		}
		c.Server.AllowedHosts = hosts
	}
	if v, ok := getenv(key(EnvKeyHSTSPreload)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyHSTSPreload), err))
		} else {
			c.Server.HSTSPreload = b
		}
	}
	if v, ok := getenv(key(EnvKeyContentSecurityPolicy)); ok {
		c.Server.ContentSecurityPolicy = v
	}
	if v, ok := getenv(key(EnvKeyDisableH2C)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyDisableH2C), err))
		} else {
			c.Server.DisableH2C = b
		}
	}
	if v, ok := getenv(key(EnvKeyDisableHTTP2)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyDisableHTTP2), err))
		} else {
			c.Server.DisableHTTP2 = b
		}
	}
	if v, ok := getenv(key(EnvKeyMaxConnections)); ok {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyMaxConnections), err))
		} else {
			c.Server.MaxConnections = n
		}
	}
	if v, ok := getenv(key(EnvKeyMaxConnectionsPerIP)); ok {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyMaxConnectionsPerIP), err))
		} else {
			c.Server.MaxConnectionsPerIP = n
		}
	}
	if v, ok := getenv(key(EnvKeyMaxRequestBodySize)); ok {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyMaxRequestBodySize), err))
		} else {
			c.Server.MaxRequestBodySize = n
		}
	}
	for _, item := range []struct {
		env string
		set func(int)
	}{
		{EnvKeyMaxHeaderListSize, func(n int) { c.Server.MaxHeaderListSize = n }},
		{EnvKeyMaxHeaderCount, func(n int) { c.Server.MaxHeaderCount = n }},
		{EnvKeyMaxRequestLineSize, func(n int) { c.Server.MaxRequestLineSize = n }},
	} {
		if v, ok := getenv(key(item.env)); ok {
			n, err := parseInt(v)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key(item.env), err))
			} else {
				item.set(n)
			}
		}
	}
	for _, item := range []struct {
		env string
		set func(int)
	}{
		{EnvKeyReadBufferSize, func(n int) { c.Server.ReadBufferSize = n }},
		{EnvKeyMaxConcurrentStreams, func(n int) {
			if n >= 0 {
				c.Server.MaxConcurrentStreams = uint32(n)
			}
		}},
	} {
		if v, ok := getenv(key(item.env)); ok {
			n, err := parseInt(v)
			if err != nil || n < 0 || (item.env == EnvKeyMaxConcurrentStreams && uint64(n) > uint64(^uint32(0))) {
				if err == nil {
					err = fmt.Errorf("value out of range")
				}
				errs = append(errs, fmt.Errorf("%s: %w", key(item.env), err))
			} else {
				item.set(n)
			}
		}
	}
	for _, item := range []struct {
		key string
		set func(string) error
	}{
		{EnvKeyMaxInFlightRequests, func(v string) error {
			n, e := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if e == nil {
				c.Server.MaxInFlightRequests = n
			}
			return e
		}},
		{EnvKeyMaxGoroutines, func(v string) error {
			n, e := parseInt(v)
			if e == nil {
				c.Server.MaxGoroutines = n
			}
			return e
		}},
		{EnvKeyMaxHeapBytes, func(v string) error {
			if _, e := parseBytes(v); e != nil {
				return e
			}
			c.Server.MaxHeapBytes = v
			return nil
		}},
		{EnvKeyResourceCheckInterval, func(v string) error {
			if _, e := time.ParseDuration(v); e != nil {
				return e
			}
			c.Server.ResourceCheckInterval = v
			return nil
		}},
		{EnvKeyMaxBufferedRequestBytes, func(v string) error {
			n, e := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if e == nil {
				c.Server.MaxBufferedRequestBytes = n
			}
			return e
		}},
		{EnvKeyMaxBufferedResponseBytes, func(v string) error {
			n, e := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
			if e == nil {
				c.Server.MaxBufferedResponseBytes = n
			}
			return e
		}},
	} {
		if v, ok := getenv(key(item.key)); ok {
			if err := item.set(v); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", key(item.key), err))
			}
		}
	}
	if v, ok := getenv(key(EnvKeyStartupBannerDisabled)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyStartupBannerDisabled), err))
		} else {
			c.StartupBanner.Disabled = b
		}
	}
	if v, ok := getenv(key(EnvKeyStartupBannerName)); ok {
		c.StartupBanner.Name = v
	}
	if v, ok := getenv(key(EnvKeyStartupBannerVersion)); ok {
		c.StartupBanner.Version = v
	}
	if v, ok := getenv(key(EnvKeyStartupBannerSubtitle)); ok {
		c.StartupBanner.Subtitle = v
	}
	if v, ok := getenv(key(EnvKeyStartupBannerScheme)); ok {
		c.StartupBanner.Scheme = v
	}
	if v, ok := getenv(key(EnvKeyStartupBannerAddress)); ok {
		c.StartupBanner.Address = v
	}
	if v, ok := getenv(key(EnvKeyStartupBannerASCIIArt)); ok {
		c.StartupBanner.ASCIIArt = v
	}
	if v, ok := getenv(key(EnvKeyStartupBannerColor)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyStartupBannerColor), err))
		} else {
			c.StartupBanner.Color = b
		}
	}
	if v, ok := getenv(key(EnvKeyReliabilityEnabled)); ok {
		b, err := parseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyReliabilityEnabled), err))
		} else {
			c.Reliability.Enabled = b
		}
	}
	if v, ok := getenv(key(EnvKeyReliabilityDataDir)); ok {
		c.Reliability.DataDir = v
	}
	if v, ok := getenv(key(EnvKeyReliabilityQueueDir)); ok {
		c.Reliability.QueueDir = v
	}
	if v, ok := getenv(key(EnvKeyReliabilityWorkers)); ok {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyReliabilityWorkers), err))
		} else {
			c.Reliability.Workers = n
		}
	}
	if v, ok := getenv(key(EnvKeyReliabilityMaxAttempts)); ok {
		n, err := parseInt(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", key(EnvKeyReliabilityMaxAttempts), err))
		} else {
			c.Reliability.MaxAttempts = n
		}
	}
	if len(errs) > 0 {
		return original, errors.Join(errs...)
	}
	if err := c.Validate(); err != nil {
		return original, err
	}
	return c, nil
}

func normalizePrefix(prefix string) (string, error) {
	trimmed := strings.TrimSpace(prefix)
	if trimmed == "" {
		return "", nil
	}
	normalized := strings.TrimRight(strings.ToUpper(trimmed), "_")
	if normalized == "" || !validEnvName(normalized) {
		return "", fmt.Errorf("config: invalid environment prefix %q", prefix)
	}
	return normalized, nil
}

func validEnvName(name string) bool {
	if name == "" || !(name[0] == '_' || name[0] >= 'A' && name[0] <= 'Z' || name[0] >= 'a' && name[0] <= 'z') {
		return false
	}
	for i := 1; i < len(name); i++ {
		ch := name[i]
		if !(ch == '_' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z' || ch >= '0' && ch <= '9') {
			return false
		}
	}
	return true
}

func parseDotEnvValue(value string) (string, error) {
	if strings.HasPrefix(value, "\"") {
		end, err := quotedValueEnd(value, '"', true)
		if err != nil {
			return "", err
		}
		if !validQuotedSuffix(value[end+1:]) {
			return "", fmt.Errorf("unexpected text after quoted value")
		}
		parsed, err := strconv.Unquote(value[:end+1])
		if err != nil {
			return "", err
		}
		return parsed, nil
	}
	if strings.HasPrefix(value, "'") {
		end, err := quotedValueEnd(value, '\'', false)
		if err != nil {
			return "", err
		}
		if !validQuotedSuffix(value[end+1:]) {
			return "", fmt.Errorf("unexpected text after quoted value")
		}
		return value[1:end], nil
	}
	if i := strings.Index(value, " #"); i >= 0 {
		value = strings.TrimSpace(value[:i])
	}
	return value, nil
}

func quotedValueEnd(value string, quote byte, escapes bool) (int, error) {
	for i := 1; i < len(value); i++ {
		if escapes && value[i] == '\\' {
			i++
			continue
		}
		if value[i] == quote {
			return i, nil
		}
	}
	return 0, fmt.Errorf("unterminated quoted value")
}

func validQuotedSuffix(suffix string) bool {
	suffix = strings.TrimSpace(suffix)
	return suffix == "" || strings.HasPrefix(suffix, "#")
}
