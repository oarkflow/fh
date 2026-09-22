package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the whole of this application's configuration, resolved from the
// environment exactly once at startup.
//
// Every field that the application cannot safely default is required, and
// Validate refuses to return a usable Config without it. This is deliberate: a
// service that starts with no signing key and fails on the first login is worse
// than one that never starts, because the second kind of failure is discovered by
// whoever deployed it rather than by a user.
type Config struct {
	Addr            string
	DatabaseURL     string
	DatabaseDriver  string
	MaxOpenConns    int
	MaxIdleConns    int
	CacheDir        string
	QueueDir        string
	JWTSecret       []byte
	JWTIssuer       string
	TokenTTL        time.Duration
	CatalogCacheTTL time.Duration
	RateLimit       int
	RateWindow      time.Duration
	Notifier        string
	SMTPHost        string
	SMTPPort        int
	SMTPFrom        string
	SMTPUser        string
	SMTPPassword    string
	WebhookURL      string
	WebhookHosts    []string
	OutboxAttempts  int
	OutboxInterval  time.Duration
	Environment     string
}

// LoadConfig reads the environment. It never reads a file and never contacts the
// network, so it is safe to call from a test.
func LoadConfig(look func(string) (string, bool)) (Config, error) {
	if look == nil {
		look = os.LookupEnv
	}
	get := func(key, fallback string) string {
		if value, ok := look(key); ok && value != "" {
			return value
		}
		return fallback
	}
	duration := func(key, fallback string) (time.Duration, error) {
		parsed, err := time.ParseDuration(get(key, fallback))
		if err != nil {
			return 0, fmt.Errorf("%s: %w", key, err)
		}
		return parsed, nil
	}
	number := func(key, fallback string) (int, error) {
		parsed, err := strconv.Atoi(get(key, fallback))
		if err != nil {
			return 0, fmt.Errorf("%s: %w", key, err)
		}
		return parsed, nil
	}

	cfg := Config{
		Addr:           get("REF_APP_ADDR", ":8090"),
		DatabaseURL:    get("DATABASE_URL", ""),
		DatabaseDriver: get("DATABASE_DRIVER", "pgx"),
		CacheDir:       get("REF_APP_CACHE_DIR", ".data/ref-app/cache"),
		QueueDir:       get("REF_APP_QUEUE_DIR", ".data/ref-app/queue"),
		JWTSecret:      []byte(get("JWT_SECRET", "")),
		JWTIssuer:      get("JWT_ISSUER", "ref-app"),
		Notifier:       strings.ToLower(get("REF_APP_NOTIFIER", "stdout")),
		SMTPHost:       get("SMTP_HOST", ""),
		SMTPFrom:       get("SMTP_FROM", "orders@example.com"),
		SMTPUser:       get("SMTP_USER", ""),
		SMTPPassword:   get("SMTP_PASSWORD", ""),
		WebhookURL:     get("REF_APP_WEBHOOK_URL", ""),
		WebhookHosts:   splitList(get("REF_APP_WEBHOOK_HOSTS", "")),
		Environment:    get("APP_ENV", "development"),
	}

	var err error
	if cfg.MaxOpenConns, err = number("REF_APP_MAX_OPEN_CONNS", "20"); err != nil {
		return Config{}, err
	}
	if cfg.MaxIdleConns, err = number("REF_APP_MAX_IDLE_CONNS", "5"); err != nil {
		return Config{}, err
	}
	if cfg.SMTPPort, err = number("SMTP_PORT", "1025"); err != nil {
		return Config{}, err
	}
	if cfg.RateLimit, err = number("REF_APP_RATE_LIMIT", "60"); err != nil {
		return Config{}, err
	}
	if cfg.OutboxAttempts, err = number("REF_APP_OUTBOX_ATTEMPTS", "8"); err != nil {
		return Config{}, err
	}
	if cfg.TokenTTL, err = duration("REF_APP_TOKEN_TTL", "1h"); err != nil {
		return Config{}, err
	}
	if cfg.CatalogCacheTTL, err = duration("REF_APP_CATALOG_TTL", "30s"); err != nil {
		return Config{}, err
	}
	if cfg.RateWindow, err = duration("REF_APP_RATE_WINDOW", "1m"); err != nil {
		return Config{}, err
	}
	if cfg.OutboxInterval, err = duration("REF_APP_OUTBOX_INTERVAL", "2s"); err != nil {
		return Config{}, err
	}
	return cfg, cfg.Validate()
}

// Validate reports every problem at once rather than the first, because fixing a
// deployment one environment variable per restart is miserable.
func (c Config) Validate() error {
	var problems []string
	if c.DatabaseURL == "" {
		problems = append(problems, "DATABASE_URL is required")
	}
	// 32 bytes is the floor for an HMAC key that is not trivially brute-forced.
	// Refusing a short one is the whole point of having a floor.
	if len(c.JWTSecret) < 32 {
		problems = append(problems, "JWT_SECRET is required and must be at least 32 bytes")
	}
	switch c.Notifier {
	case "stdout":
		// Explicitly chosen, and it says what it is: deliveries are printed.
	case "smtp":
		if c.SMTPHost == "" {
			problems = append(problems, "REF_APP_NOTIFIER=smtp needs SMTP_HOST")
		}
	case "webhook":
		if c.WebhookURL == "" {
			problems = append(problems, "REF_APP_NOTIFIER=webhook needs REF_APP_WEBHOOK_URL")
		}
		if len(c.WebhookHosts) == 0 {
			problems = append(problems, "REF_APP_NOTIFIER=webhook needs REF_APP_WEBHOOK_HOSTS, the allowlist of hosts it may reach")
		}
	default:
		problems = append(problems, fmt.Sprintf("REF_APP_NOTIFIER=%q is not one of stdout, smtp, webhook", c.Notifier))
	}
	if c.RateLimit <= 0 {
		problems = append(problems, "REF_APP_RATE_LIMIT must be positive")
	}
	if len(problems) > 0 {
		return fmt.Errorf("ref-app: configuration is incomplete:\n  - %s", strings.Join(problems, "\n  - "))
	}
	return nil
}

func splitList(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}
