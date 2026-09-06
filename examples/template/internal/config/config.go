package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/bcl"
	"github.com/oarkflow/fh"
	fhconfig "github.com/oarkflow/fh/pkg/config"
)

type Config struct {
	Name            string
	Addr            string
	Environment     string
	Debug           bool
	AllowedHosts    []string
	DevelopmentAuth bool
	AuthConfigured  bool
	RequestTimeout  time.Duration
	MaxBodyBytes    int
	RateLimit       int
	DatabaseEnabled bool
	DatabaseDriver  string
	DatabaseDSN     string
	DatabaseName    string
	JWTSecret       string
	SessionSecret   string
	BootstrapUser   string
	BootstrapPass   string
}

type bclConfig struct {
	Name            string `bcl:"name"`
	Addr            string `bcl:"addr"`
	Environment     string `bcl:"environment"`
	Debug           bool   `bcl:"debug"`
	AllowedHosts    string `bcl:"allowed_hosts"`
	DevelopmentAuth bool   `bcl:"development_auth"`
	AuthConfigured  bool   `bcl:"auth_configured"`
	RequestTimeout  string `bcl:"request_timeout"`
	MaxBodyBytes    int    `bcl:"max_body_bytes"`
	RateLimit       int    `bcl:"rate_limit"`
	DatabaseEnabled bool   `bcl:"database_enabled"`
	DatabaseDriver  string `bcl:"database_driver"`
	DatabaseDSN     string `bcl:"database_dsn"`
	DatabaseName    string `bcl:"database_name"`
	BootstrapUser   string `bcl:"bootstrap_user"`
}

func Load() (Config, error) {
	c := Config{Name: env("APP_NAME", "fh-template"), Addr: env("APP_ADDR", ":8080"), Environment: env("APP_ENV", "development"), AllowedHosts: []string{"localhost", "127.0.0.1", "::1"}, RequestTimeout: 30 * time.Second, MaxBodyBytes: 4 << 20, RateLimit: 100, DatabaseEnabled: true, DatabaseDriver: "sqlite", DatabaseDSN: "file:app.db?_pragma=foreign_keys(1)", DatabaseName: "app", BootstrapUser: "admin"}
	configPath := env("APP_CONFIG", "config.bcl")
	if info, statErr := os.Stat(configPath); statErr == nil {
		if info.IsDir() {
			return Config{}, fmt.Errorf("APP_CONFIG %q is a directory", configPath)
		}
		var fileCfg bclConfig
		if err := bcl.DecodeFile(filepath.Clean(configPath), &fileCfg); err != nil {
			return Config{}, fmt.Errorf("load BCL config %q: %w", configPath, err)
		}
		if fileCfg.Name != "" {
			c.Name = fileCfg.Name
		}
		if fileCfg.Addr != "" {
			c.Addr = fileCfg.Addr
		}
		if fileCfg.Environment != "" {
			c.Environment = fileCfg.Environment
		}
		c.Debug, c.DevelopmentAuth, c.AuthConfigured = fileCfg.Debug, fileCfg.DevelopmentAuth, fileCfg.AuthConfigured
		c.AllowedHosts = split(fileCfg.AllowedHosts)
		if fileCfg.RequestTimeout != "" {
			c.RequestTimeout, _ = time.ParseDuration(fileCfg.RequestTimeout)
		}
		if fileCfg.MaxBodyBytes > 0 {
			c.MaxBodyBytes = fileCfg.MaxBodyBytes
		}
		if fileCfg.RateLimit > 0 {
			c.RateLimit = fileCfg.RateLimit
		}
		c.DatabaseEnabled = fileCfg.DatabaseEnabled
		if fileCfg.DatabaseDriver != "" {
			c.DatabaseDriver = fileCfg.DatabaseDriver
		}
		if fileCfg.DatabaseDSN != "" {
			c.DatabaseDSN = fileCfg.DatabaseDSN
		}
		if fileCfg.DatabaseName != "" {
			c.DatabaseName = fileCfg.DatabaseName
		}
		if fileCfg.BootstrapUser != "" {
			c.BootstrapUser = fileCfg.BootstrapUser
		}
	} else if !os.IsNotExist(statErr) || hasEnv("APP_CONFIG") {
		return Config{}, fmt.Errorf("stat BCL config %q: %w", configPath, statErr)
	}
	if hasEnv("APP_DEBUG") {
		c.Debug = boolEnv("APP_DEBUG", c.Environment != "production")
	}
	if hasEnv("APP_DEV_AUTH") {
		c.DevelopmentAuth = boolEnv("APP_DEV_AUTH", false)
	}
	if hasEnv("APP_AUTH_CONFIGURED") {
		c.AuthConfigured = boolEnv("APP_AUTH_CONFIGURED", false)
	}
	if hasEnv("APP_DATABASE_ENABLED") {
		c.DatabaseEnabled = boolEnv("APP_DATABASE_ENABLED", false)
	}
	c.DatabaseDriver = env("APP_DATABASE_DRIVER", c.DatabaseDriver)
	c.DatabaseDSN = env("APP_DATABASE_DSN", c.DatabaseDSN)
	c.DatabaseName = env("APP_DATABASE_NAME", c.DatabaseName)
	c.BootstrapUser = env("APP_BOOTSTRAP_USERNAME", c.BootstrapUser)
	var secretErr error
	c.JWTSecret, secretErr = Secret("APP_AUTH_JWT_SECRET", "APP_AUTH_JWT_SECRET_FILE")
	if secretErr != nil && c.Environment == "production" && c.AuthConfigured {
		return Config{}, secretErr
	}
	c.SessionSecret, secretErr = Secret("APP_SESSION_SECRET", "APP_SESSION_SECRET_FILE")
	if secretErr != nil && c.Environment == "production" {
		return Config{}, secretErr
	}
	c.BootstrapPass, secretErr = Secret("APP_BOOTSTRAP_PASSWORD", "APP_BOOTSTRAP_PASSWORD_FILE")
	if secretErr != nil && hasEnv("APP_BOOTSTRAP_PASSWORD_FILE") {
		return Config{}, secretErr
	}
	if hasEnv("APP_ALLOWED_HOSTS") {
		c.AllowedHosts = split(os.Getenv("APP_ALLOWED_HOSTS"))
	}
	var err error
	if c.RequestTimeout, err = durationEnv("APP_REQUEST_TIMEOUT", c.RequestTimeout); err != nil {
		return Config{}, err
	}
	if c.MaxBodyBytes, err = intEnv("APP_MAX_BODY_BYTES", c.MaxBodyBytes); err != nil {
		return Config{}, err
	}
	if c.RateLimit, err = intEnv("APP_RATE_LIMIT", c.RateLimit); err != nil {
		return Config{}, err
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	// Never allow generator markers to reach logs, headers, or service metadata.
	if strings.Contains(c.Name, "{{APP_NAME}}") {
		c.Name = "fh-template"
	}
	return c, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Name) == "" || strings.TrimSpace(c.Addr) == "" {
		return errors.New("APP_NAME and APP_ADDR are required")
	}
	if c.Environment != "development" && c.Environment != "test" && c.Environment != "production" {
		return fmt.Errorf("APP_ENV must be development, test, or production")
	}
	if c.RequestTimeout <= 0 || c.MaxBodyBytes <= 0 || c.RateLimit <= 0 {
		return errors.New("request timeout, body limit, and rate limit must be positive")
	}
	if c.DatabaseEnabled && (strings.TrimSpace(c.DatabaseDriver) == "" || strings.TrimSpace(c.DatabaseDSN) == "") {
		return errors.New("database driver and DSN are required when database is enabled")
	}
	if c.Environment == "production" && c.AuthConfigured && len(c.JWTSecret) < 32 {
		return errors.New("production authentication requires APP_AUTH_JWT_SECRET or APP_AUTH_JWT_SECRET_FILE with at least 32 bytes")
	}
	if c.Environment == "production" && len(c.SessionSecret) < 32 {
		return errors.New("production sessions require APP_SESSION_SECRET or APP_SESSION_SECRET_FILE with at least 32 bytes")
	}
	if c.Environment == "production" && c.DevelopmentAuth {
		return errors.New("APP_DEV_AUTH cannot be enabled in production")
	}
	if c.Environment == "production" && !c.AuthConfigured {
		return errors.New("production requires an explicit authentication adapter; set APP_AUTH_CONFIGURED=true after wiring one")
	}
	if c.DevelopmentAuth {
		for _, host := range c.AllowedHosts {
			if ip := net.ParseIP(host); ip != nil && !ip.IsLoopback() {
				return errors.New("development authentication requires loopback-only hosts")
			}
		}
	}
	return nil
}

func Framework(c Config) (fh.Config, error) {
	env := "production"
	if c.Environment != "production" {
		env = "development"
	}
	return fh.Config{ReadTimeout: 10 * time.Second, ReadHeaderTimeout: 5 * time.Second, RequestBodyTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second, HandlerTimeout: c.RequestTimeout, IdleTimeout: 120 * time.Second, TLSHandshakeTimeout: 10 * time.Second, HTTP2IdleTimeout: 120 * time.Second, MaxConnections: 10000, MaxConnectionsPerIP: 100, MaxRequestBodySize: c.MaxBodyBytes, MaxHeaderListSize: 64 << 10, MaxHeaderCount: 64, MaxRequestLineSize: 8 << 10, SecureByDefault: true, DisableH2C: true, Debug: c.Debug, Environment: map[string]fh.Environment{"production": fh.EnvProduction, "development": fh.EnvDevelopment, "test": fh.EnvDevelopment}[env], StartupBanner: fh.StartupBannerConfig{Disabled: true, Name: c.Name}}, nil
}

func Secret(valueEnv, fileEnv string) (string, error) {
	return fhconfig.SecretString(valueEnv, fileEnv)
}
func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
func hasEnv(key string) bool { _, ok := os.LookupEnv(key); return ok }
func boolEnv(key string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
func intEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}
func durationEnv(key string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}
func split(value string) []string {
	var out []string
	for _, item := range strings.Split(value, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	return out
}
