package config

import (
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	fhconfig "github.com/oarkflow/fh/pkg/config"
)

type Config struct {
	Environment               string
	Production                bool
	Address                   string
	Origin                    string
	AllowedHosts              []string
	AllowedOrigins            []string
	DatabaseURL               string
	SessionSecret             []byte
	LoginUser                 string
	LoginPassword             string
	TransportPrivateKey       string
	TransportKeyID            string
	ResponseSigningPrivateKey string
	ResponseSigningKeyID      string
	OperationsToken           string
	RequestTimeout            time.Duration
	MaxBodyBytes              int
	RateLimit                 int
}

func Load() (Config, error) {
	if err := loadDotEnv(".env"); err != nil {
		return Config{}, err
	}
	environment := env("APP_ENV", "development")
	// Only an explicit development environment gets development concessions.
	production := !strings.EqualFold(environment, "development")
	origin, allowedHosts, allowedOrigins, err := parseOriginPolicy(env("APP_ORIGIN", ":8080"), production)
	if err != nil {
		return Config{}, err
	}

	sessionValue, err := fhconfig.SecretString("APP_SESSION_SECRET", "APP_SESSION_SECRET_FILE")
	if err != nil {
		return Config{}, err
	}
	sessionSecret, err := base64.RawURLEncoding.DecodeString(sessionValue)
	if err != nil || len(sessionSecret) < 32 {
		return Config{}, fmt.Errorf("APP_SESSION_SECRET must be at least 32 random base64url bytes")
	}
	loginPassword, err := fhconfig.SecretString("APP_LOGIN_PASSWORD", "APP_LOGIN_PASSWORD_FILE")
	if err != nil {
		return Config{}, err
	}
	transportKey, err := fhconfig.SecretString("APP_SECURE_TRANSPORT_KEY", "APP_SECURE_TRANSPORT_KEY_FILE")
	if err != nil {
		return Config{}, err
	}
	responseKey, err := fhconfig.SecretString("APP_RESPONSE_SIGNING_PRIVATE_KEY", "APP_RESPONSE_SIGNING_PRIVATE_KEY_FILE")
	if err != nil {
		return Config{}, err
	}
	operationsToken, err := fhconfig.SecretString("APP_OPERATIONS_TOKEN", "APP_OPERATIONS_TOKEN_FILE")
	if err != nil {
		return Config{}, err
	}
	if production {
		for name, value := range map[string]string{
			"APP_LOGIN_PASSWORD_FILE":               loginPassword,
			"APP_SECURE_TRANSPORT_KEY_FILE":         transportKey,
			"APP_RESPONSE_SIGNING_PRIVATE_KEY_FILE": responseKey,
			"APP_OPERATIONS_TOKEN_FILE":             operationsToken,
		} {
			if value == "" {
				return Config{}, fmt.Errorf("%s is required in production", name)
			}
		}
	}
	timeout, err := time.ParseDuration(env("APP_REQUEST_TIMEOUT", "15s"))
	if err != nil || timeout <= 0 {
		return Config{}, fmt.Errorf("APP_REQUEST_TIMEOUT must be a positive duration")
	}
	maxBody, err := positiveInt("APP_MAX_BODY_BYTES", 1<<20)
	if err != nil {
		return Config{}, err
	}
	rateLimit, err := positiveInt("APP_RATE_LIMIT", 120)
	if err != nil {
		return Config{}, err
	}

	return Config{
		Environment: environment, Production: production, Address: env("APP_ADDR", "127.0.0.1:8080"), Origin: origin,
		AllowedHosts: allowedHosts, AllowedOrigins: allowedOrigins, DatabaseURL: env("APP_DATABASE_URL", "file:data/app.db?_pragma=foreign_keys(1)"),
		SessionSecret: sessionSecret, LoginUser: env("APP_LOGIN_USER", "admin"), LoginPassword: loginPassword,
		TransportPrivateKey: transportKey, TransportKeyID: env("APP_SECURE_TRANSPORT_KEY_ID", "app-transport-v1"),
		ResponseSigningPrivateKey: responseKey, ResponseSigningKeyID: env("APP_RESPONSE_SIGNING_KEY_ID", "app-response-v1"),
		OperationsToken: operationsToken, RequestTimeout: timeout, MaxBodyBytes: maxBody, RateLimit: rateLimit,
	}, nil
}

func positiveInt(name string, fallback int) (int, error) {
	value, err := strconv.Atoi(env(name, strconv.Itoa(fallback)))
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func parseOriginPolicy(value string, production bool) (string, []string, []string, error) {
	raw := strings.TrimSpace(strings.TrimRight(value, "/"))
	wildcardDevelopment := strings.HasPrefix(raw, ":")
	if wildcardDevelopment {
		if production {
			return "", nil, nil, fmt.Errorf("APP_ORIGIN must be an absolute HTTPS origin in production")
		}
		raw = "http://localhost" + raw
	} else if !strings.Contains(raw, "://") {
		if production {
			return "", nil, nil, fmt.Errorf("APP_ORIGIN must be an absolute HTTPS origin in production")
		}
		raw = "http://" + raw
	}

	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", nil, nil, fmt.Errorf("APP_ORIGIN must be :port, host:port, or an absolute origin without path, query, credentials, or fragment")
	}
	if parsed.Port() != "" {
		port, portErr := strconv.Atoi(parsed.Port())
		if portErr != nil || port < 1 || port > 65535 {
			return "", nil, nil, fmt.Errorf("APP_ORIGIN has an invalid port")
		}
	}
	if production && !strings.EqualFold(parsed.Scheme, "https") {
		return "", nil, nil, fmt.Errorf("APP_ORIGIN must use HTTPS in production")
	}

	host := strings.ToLower(parsed.Hostname())
	loopback := host == "localhost" || net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
	if !production && !strings.EqualFold(parsed.Scheme, "https") && !loopback && !wildcardDevelopment {
		return "", nil, nil, fmt.Errorf("APP_ORIGIN must use HTTPS outside loopback development; use :port to explicitly enable LAN development")
	}

	hosts := []string{host}
	if loopback || wildcardDevelopment {
		hosts = []string{"localhost", "127.0.0.1", "::1"}
	}
	if wildcardDevelopment {
		hosts = append(hosts, developmentLANHosts()...)
	}
	hosts = uniqueStrings(hosts)

	origins := make([]string, 0, len(hosts))
	for _, allowedHost := range hosts {
		originHost := allowedHost
		if parsed.Port() != "" {
			originHost = net.JoinHostPort(allowedHost, parsed.Port())
		} else if strings.Contains(allowedHost, ":") {
			originHost = "[" + allowedHost + "]"
		}
		origins = append(origins, strings.ToLower(parsed.Scheme)+"://"+originHost)
	}
	return strings.ToLower(parsed.Scheme) + "://" + parsed.Host, hosts, uniqueStrings(origins), nil
}

func developmentLANHosts() []string {
	hosts := []string{"0.0.0.0"}
	if hostname, err := os.Hostname(); err == nil && strings.TrimSpace(hostname) != "" {
		hosts = append(hosts, hostname)
	}
	if addresses, err := net.InterfaceAddrs(); err == nil {
		for _, address := range addresses {
			ip, _, err := net.ParseCIDR(address.String())
			if err == nil && !ip.IsUnspecified() && !ip.IsMulticast() {
				hosts = append(hosts, ip.String())
			}
		}
	}
	return hosts
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
