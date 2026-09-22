package platform

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/smtp"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/platform/spi"
)

// Outbound service providers.
//
// Every outbound call this platform makes goes through one of these, and every
// one of them enforces the same three things:
//
//   - A host allowlist. An application author configures a URL; a compromised or
//     mistaken configuration must not be able to turn the server into a proxy
//     for arbitrary destinations, which is what server-side request forgery is.
//     The allowlist is required, not optional, and it is checked after DNS
//     resolution against the resolved address as well as the hostname.
//   - A response size limit. An unbounded read of somebody else's response is a
//     memory exhaustion vector they control.
//   - A timeout. Every call has one; there is no way to configure "wait forever".

func registerServiceResources(r *Registry) {
	mustResource(r, "service.http", ResourceFactoryFunc(openHTTPService), ResourceKindInfo{
		Family:   "service",
		Summary:  "Outbound HTTP client restricted to an allowlist of hosts, with retries, timeouts and optional request signing.",
		Provides: []string{"HTTPService"},
		Config: []ConfigField{
			{Name: "base_url", Type: "string", Summary: "Prefix for relative node URLs"},
			{Name: "allowed_hosts", Type: "[]string", Required: true, Summary: "Hostnames this client may reach. Required — there is no wildcard."},
			{Name: "timeout", Type: "duration", Default: "10s"},
			{Name: "max_response_bytes", Type: "int", Default: "1048576"},
			{Name: "headers", Type: "map", Summary: "Headers added to every request"},
			{Name: "retry_attempts", Type: "int", Default: "1", Summary: "Total attempts, including the first"},
			{Name: "retry_backoff", Type: "duration", Default: "200ms"},
			{Name: "allow_private_networks", Type: "bool", Default: "false", Summary: "Permit loopback and RFC1918 destinations. Development only."},
			{Name: "sign_secret", Type: "string", Summary: "HMAC key; when set every request is signed"},
			{Name: "sign_header", Type: "string", Default: "X-Signature"},
			{Name: "client_cert_file", Type: "string", Summary: "mTLS client certificate"},
			{Name: "client_key_file", Type: "string"},
			{Name: "ca_file", Type: "string", Summary: "Trust roots, when the peer is not publicly signed"},
			{Name: "insecure_skip_verify", Type: "bool", Default: "false"},
		},
	})

	mustResource(r, "service.smtp", ResourceFactoryFunc(openSMTPService), ResourceKindInfo{
		Family:   "service",
		Summary:  "Transactional mail over SMTP with STARTTLS.",
		Provides: []string{"Mailer"},
		Config: []ConfigField{
			{Name: "host", Type: "string", Required: true},
			{Name: "port", Type: "int", Default: "587"},
			{Name: "username", Type: "string"},
			{Name: "password", Type: "string"},
			{Name: "from", Type: "string", Required: true, Summary: "Default From address"},
			{Name: "timeout", Type: "duration", Default: "20s"},
			{Name: "tls", Type: "string", Default: "starttls", Summary: "starttls, implicit or none"},
			{Name: "insecure_skip_verify", Type: "bool", Default: "false"},
		},
	})

	mustResource(r, "service.llm", ResourceFactoryFunc(openLLMService), ResourceKindInfo{
		Family:   "service",
		Summary:  "Chat and embedding calls to an OpenAI-shaped or Anthropic-shaped HTTP API.",
		Provides: []string{"LLM", "Embedder"},
		Config: []ConfigField{
			{Name: "api", Type: "string", Default: "openai", Summary: `"openai" or "anthropic" request shape`},
			{Name: "base_url", Type: "string", Required: true},
			{Name: "api_key", Type: "string", Required: true},
			{Name: "model", Type: "string", Required: true, Summary: "Default model"},
			{Name: "embedding_model", Type: "string"},
			{Name: "allowed_hosts", Type: "[]string", Required: true},
			{Name: "timeout", Type: "duration", Default: "60s"},
			{Name: "max_response_bytes", Type: "int", Default: "4194304"},
			{Name: "max_tokens", Type: "int", Default: "1024"},
			{Name: "anthropic_version", Type: "string", Default: "2023-06-01"},
		},
	})
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

// HTTPService is the handle a service.http resource exposes.
type HTTPService struct {
	client         *http.Client
	baseURL        string
	allowedHosts   map[string]struct{}
	defaultHeaders map[string]string
	maxBytes       int64
	attempts       int
	backoff        time.Duration
	allowPrivate   bool
	signSecret     []byte
	signHeader     string
}

// HTTPRequest is one outbound call.
type HTTPRequest struct {
	Method  string
	URL     string
	Headers map[string]string
	Query   map[string]string
	Body    []byte
}

// HTTPResponse is what came back, with the body already read and bounded.
type HTTPResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"-"`
	JSON    any               `json:"body,omitempty"`
	Text    string            `json:"text,omitempty"`
}

func openHTTPService(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("service.http", spec.Config,
		"base_url", "allowed_hosts", "timeout", "max_response_bytes", "headers",
		"retry_attempts", "retry_backoff", "allow_private_networks",
		"sign_secret", "sign_header",
		"client_cert_file", "client_key_file", "ca_file", "insecure_skip_verify"); err != nil {
		return nil, nil, err
	}
	timeout, err := configDuration(spec.Config, "timeout", 10*time.Second)
	if err != nil {
		return nil, nil, err
	}
	maxBytes, err := configInt64(spec.Config, "max_response_bytes", 1<<20)
	if err != nil {
		return nil, nil, err
	}
	attempts, err := configInt(spec.Config, "retry_attempts", 1)
	if err != nil {
		return nil, nil, err
	}
	backoff, err := configDuration(spec.Config, "retry_backoff", 200*time.Millisecond)
	if err != nil {
		return nil, nil, err
	}

	service := &HTTPService{
		baseURL:        strings.TrimSuffix(configString(spec.Config, "base_url", ""), "/"),
		allowedHosts:   map[string]struct{}{},
		defaultHeaders: stringMap(spec.Config["headers"]),
		maxBytes:       maxBytes,
		attempts:       max(attempts, 1),
		backoff:        backoff,
		allowPrivate:   configBool(spec.Config, "allow_private_networks", false),
		signHeader:     configString(spec.Config, "sign_header", "X-Signature"),
	}
	for _, host := range configStrings(spec.Config, "allowed_hosts") {
		service.allowedHosts[strings.ToLower(strings.TrimSpace(host))] = struct{}{}
	}
	if len(service.allowedHosts) == 0 {
		return nil, nil, fmt.Errorf("resource %q: service.http requires config.allowed_hosts — an outbound client with no destination allowlist is a server-side request forgery primitive", spec.Name)
	}
	if secret := configString(spec.Config, "sign_secret", ""); secret != "" {
		if len(secret) < 32 {
			return nil, nil, fmt.Errorf("resource %q: sign_secret must contain at least 32 bytes", spec.Name)
		}
		service.signSecret = []byte(secret)
	}

	transport, err := buildTransport(spec)
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	service.client = &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// A redirect is a destination the allowlist never approved, so each
			// hop is re-checked. Without this, an allowed host could bounce the
			// client anywhere.
			if err := service.checkHost(req.URL); err != nil {
				return err
			}
			if len(via) >= 5 {
				return fmt.Errorf("too many redirects")
			}
			return nil
		},
	}
	return service, nil, nil
}

func buildTransport(spec ResourceSpec) (http.RoundTripper, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	configured := false

	if certFile := configString(spec.Config, "client_cert_file", ""); certFile != "" {
		keyFile := configString(spec.Config, "client_key_file", "")
		if keyFile == "" {
			return nil, fmt.Errorf("client_cert_file also needs client_key_file")
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
		configured = true
	}
	if caFile := configString(spec.Config, "ca_file", ""); caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("ca_file contains no usable certificates")
		}
		tlsConfig.RootCAs = pool
		configured = true
	}
	if configBool(spec.Config, "insecure_skip_verify", false) {
		tlsConfig.InsecureSkipVerify = true
		configured = true
	}
	if !configured {
		return http.DefaultTransport, nil
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	return transport, nil
}

// Do performs one request, applying the allowlist, retries and size limit.
func (s *HTTPService) Do(ctx context.Context, request HTTPRequest) (HTTPResponse, error) {
	target, err := s.resolve(request.URL)
	if err != nil {
		return HTTPResponse{}, err
	}
	if len(request.Query) > 0 {
		query := target.Query()
		for key, value := range request.Query {
			query.Set(key, value)
		}
		target.RawQuery = query.Encode()
	}
	if err := s.checkHost(target); err != nil {
		return HTTPResponse{}, err
	}

	method := strings.ToUpper(request.Method)
	if method == "" {
		method = http.MethodGet
	}

	var lastErr error
	for attempt := 1; attempt <= s.attempts; attempt++ {
		response, err := s.attempt(ctx, method, target, request)
		if err == nil {
			return response, nil
		}
		lastErr = err
		// Only transport-level failures and 5xx are retried. Retrying a 4xx
		// would repeat a request the peer has already told us is wrong.
		if attempt == s.attempts || !retriableHTTP(err) {
			break
		}
		select {
		case <-ctx.Done():
			return HTTPResponse{}, ctx.Err()
		case <-time.After(s.backoff * time.Duration(attempt)):
		}
	}
	return HTTPResponse{}, lastErr
}

func (s *HTTPService) attempt(ctx context.Context, method string, target *url.URL, request HTTPRequest) (HTTPResponse, error) {
	var body io.Reader
	if len(request.Body) > 0 {
		body = strings.NewReader(string(request.Body))
	}
	httpRequest, err := http.NewRequestWithContext(ctx, method, target.String(), body)
	if err != nil {
		return HTTPResponse{}, err
	}
	for key, value := range s.defaultHeaders {
		httpRequest.Header.Set(key, value)
	}
	for key, value := range request.Headers {
		httpRequest.Header.Set(key, value)
	}
	if len(request.Body) > 0 && httpRequest.Header.Get("Content-Type") == "" {
		httpRequest.Header.Set("Content-Type", "application/json")
	}
	if len(s.signSecret) > 0 {
		timestamp := strings.TrimSpace(fmt.Sprint(time.Now().UTC().Unix()))
		mac := hmac.New(sha256.New, s.signSecret)
		mac.Write([]byte(timestamp))
		mac.Write([]byte("."))
		mac.Write(request.Body)
		httpRequest.Header.Set("X-Signature-Timestamp", timestamp)
		httpRequest.Header.Set(s.signHeader, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	}

	response, err := s.client.Do(httpRequest)
	if err != nil {
		return HTTPResponse{}, &httpCallError{retriable: true, cause: err}
	}
	defer response.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(response.Body, s.maxBytes+1))
	if err != nil {
		return HTTPResponse{}, &httpCallError{retriable: true, cause: err}
	}
	if int64(len(raw)) > s.maxBytes {
		return HTTPResponse{}, fmt.Errorf("response exceeds the %d byte limit configured for this service", s.maxBytes)
	}

	result := HTTPResponse{
		Status:  response.StatusCode,
		Body:    raw,
		Headers: map[string]string{},
	}
	for key := range response.Header {
		result.Headers[key] = response.Header.Get(key)
	}
	if strings.Contains(response.Header.Get("Content-Type"), "json") && len(raw) > 0 {
		_ = json.Unmarshal(raw, &result.JSON)
	}
	if result.JSON == nil {
		result.Text = string(raw)
	}

	if response.StatusCode >= 500 {
		return result, &httpCallError{retriable: true, status: response.StatusCode, cause: fmt.Errorf("upstream returned %s", response.Status)}
	}
	if response.StatusCode >= 400 {
		return result, &httpCallError{status: response.StatusCode, cause: fmt.Errorf("upstream returned %s", response.Status)}
	}
	return result, nil
}

func (s *HTTPService) resolve(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, fmt.Errorf("no URL configured for this call")
	}
	if strings.HasPrefix(raw, "/") && s.baseURL != "" {
		raw = s.baseURL + raw
	}
	target, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, fmt.Errorf("only http and https URLs may be called, got %q", target.Scheme)
	}
	return target, nil
}

// checkHost enforces the allowlist and, unless explicitly permitted, refuses
// private and loopback destinations.
//
// The address check matters as much as the name check: an allowlisted hostname
// whose DNS record points at 169.254.169.254 would otherwise reach a cloud
// metadata service, which is the classic escalation from SSRF to credentials.
func (s *HTTPService) checkHost(target *url.URL) error {
	host := strings.ToLower(target.Hostname())
	if _, allowed := s.allowedHosts[host]; !allowed {
		return fmt.Errorf("host %q is not in this service's allowed_hosts", host)
	}
	if s.allowPrivate {
		return nil
	}
	addresses, err := net.LookupIP(host)
	if err != nil {
		return fmt.Errorf("resolve %q: %w", host, err)
	}
	for _, address := range addresses {
		if address.IsLoopback() || address.IsPrivate() || address.IsLinkLocalUnicast() || address.IsUnspecified() {
			return fmt.Errorf("host %q resolves to the private address %s; set allow_private_networks to permit this", host, address)
		}
	}
	return nil
}

type httpCallError struct {
	retriable bool
	status    int
	cause     error
}

func (e *httpCallError) Error() string { return e.cause.Error() }
func (e *httpCallError) Unwrap() error { return e.cause }

func retriableHTTP(err error) bool {
	var callErr *httpCallError
	if errors.As(err, &callErr) {
		return callErr.retriable
	}
	return false
}

// ---------------------------------------------------------------------------
// SMTP
// ---------------------------------------------------------------------------

type smtpService struct {
	addr     string
	host     string
	auth     smtp.Auth
	from     string
	timeout  time.Duration
	mode     string
	insecure bool
}

func openSMTPService(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("service.smtp", spec.Config,
		"host", "port", "username", "password", "from", "timeout", "tls", "insecure_skip_verify"); err != nil {
		return nil, nil, err
	}
	host, err := requiredString(spec.Config, "host")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	from, err := requiredString(spec.Config, "from")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	port, err := configInt(spec.Config, "port", 587)
	if err != nil {
		return nil, nil, err
	}
	timeout, err := configDuration(spec.Config, "timeout", 20*time.Second)
	if err != nil {
		return nil, nil, err
	}
	mode := strings.ToLower(configString(spec.Config, "tls", "starttls"))
	switch mode {
	case "starttls", "implicit", "none":
	default:
		return nil, nil, fmt.Errorf("resource %q: tls must be starttls, implicit or none", spec.Name)
	}

	service := &smtpService{
		addr:     net.JoinHostPort(host, fmt.Sprint(port)),
		host:     host,
		from:     from,
		timeout:  timeout,
		mode:     mode,
		insecure: configBool(spec.Config, "insecure_skip_verify", false),
	}
	if username := configString(spec.Config, "username", ""); username != "" {
		password := configString(spec.Config, "password", "")
		// PlainAuth refuses to send credentials over an unencrypted connection,
		// which is the behaviour we want: if TLS is off, so is authentication.
		service.auth = smtp.PlainAuth("", username, password, host)
	}
	return service, nil, nil
}

// Send implements spi.Mailer.
func (s *smtpService) Send(ctx context.Context, message spi.Mail) (string, error) {
	if len(message.To) == 0 {
		return "", fmt.Errorf("a message needs at least one recipient")
	}
	from := message.From
	if from == "" {
		from = s.from
	}
	messageID := fmt.Sprintf("<%s@%s>", newRandomID(), s.host)
	body := buildMIME(messageID, from, message)

	client, err := s.dial(ctx)
	if err != nil {
		return "", err
	}
	defer func() { _ = client.Quit() }()

	if s.auth != nil {
		if err := client.Auth(s.auth); err != nil {
			return "", fmt.Errorf("SMTP authentication: %w", err)
		}
	}
	if err := client.Mail(from); err != nil {
		return "", err
	}
	recipients := append(append(append([]string{}, message.To...), message.CC...), message.BCC...)
	for _, recipient := range recipients {
		if err := client.Rcpt(recipient); err != nil {
			return "", fmt.Errorf("recipient %q: %w", recipient, err)
		}
	}
	writer, err := client.Data()
	if err != nil {
		return "", err
	}
	if _, err := writer.Write(body); err != nil {
		writer.Close()
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	return messageID, nil
}

func (s *smtpService) dial(ctx context.Context) (*smtp.Client, error) {
	dialer := &net.Dialer{Timeout: s.timeout}
	tlsConfig := &tls.Config{ServerName: s.host, MinVersion: tls.VersionTLS12, InsecureSkipVerify: s.insecure}

	if s.mode == "implicit" {
		conn, err := tls.DialWithDialer(dialer, "tcp", s.addr, tlsConfig)
		if err != nil {
			return nil, err
		}
		return smtp.NewClient(conn, s.host)
	}
	conn, err := dialer.DialContext(ctx, "tcp", s.addr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(s.timeout))
	client, err := smtp.NewClient(conn, s.host)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if s.mode == "starttls" {
		if ok, _ := client.Extension("STARTTLS"); !ok {
			client.Close()
			return nil, fmt.Errorf("the SMTP server does not offer STARTTLS; set tls \"none\" only if the link is already private")
		}
		if err := client.StartTLS(tlsConfig); err != nil {
			client.Close()
			return nil, err
		}
	}
	return client, nil
}

// buildMIME renders a message. Header values are sanitised of CR and LF, which
// is what stops a templated subject or recipient from injecting extra headers.
func buildMIME(messageID, from string, message spi.Mail) []byte {
	var out strings.Builder
	header := func(name, value string) {
		if value == "" {
			return
		}
		out.WriteString(name)
		out.WriteString(": ")
		out.WriteString(sanitizeHeader(value))
		out.WriteString("\r\n")
	}
	header("Message-ID", messageID)
	header("Date", time.Now().UTC().Format(time.RFC1123Z))
	header("From", from)
	header("To", strings.Join(message.To, ", "))
	header("Cc", strings.Join(message.CC, ", "))
	header("Reply-To", message.ReplyTo)
	header("Subject", message.Subject)
	for name, value := range message.Headers {
		header(name, value)
	}
	out.WriteString("MIME-Version: 1.0\r\n")

	switch {
	case message.HTML != "" && message.Body != "":
		boundary := newToken(16)
		out.WriteString("Content-Type: multipart/alternative; boundary=\"")
		out.WriteString(boundary)
		out.WriteString("\"\r\n\r\n--")
		out.WriteString(boundary)
		out.WriteString("\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n")
		out.WriteString(message.Body)
		out.WriteString("\r\n--")
		out.WriteString(boundary)
		out.WriteString("\r\nContent-Type: text/html; charset=UTF-8\r\n\r\n")
		out.WriteString(message.HTML)
		out.WriteString("\r\n--")
		out.WriteString(boundary)
		out.WriteString("--\r\n")
	case message.HTML != "":
		out.WriteString("Content-Type: text/html; charset=UTF-8\r\n\r\n")
		out.WriteString(message.HTML)
	default:
		out.WriteString("Content-Type: text/plain; charset=UTF-8\r\n\r\n")
		out.WriteString(message.Body)
	}
	return []byte(out.String())
}

func sanitizeHeader(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(value)
}

// ---------------------------------------------------------------------------
// Language models
// ---------------------------------------------------------------------------

type llmService struct {
	http           *HTTPService
	api            string
	model          string
	embeddingModel string
	maxTokens      int
	apiKey         string
	version        string
}

func openLLMService(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("service.llm", spec.Config,
		"api", "base_url", "api_key", "model", "embedding_model", "allowed_hosts",
		"timeout", "max_response_bytes", "max_tokens", "anthropic_version"); err != nil {
		return nil, nil, err
	}
	api := strings.ToLower(configString(spec.Config, "api", "openai"))
	if api != "openai" && api != "anthropic" {
		return nil, nil, fmt.Errorf("resource %q: api must be \"openai\" or \"anthropic\"", spec.Name)
	}
	apiKey, err := requiredString(spec.Config, "api_key")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	model, err := requiredString(spec.Config, "model")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	maxTokens, err := configInt(spec.Config, "max_tokens", 1024)
	if err != nil {
		return nil, nil, err
	}

	httpSpec := spec
	httpSpec.Kind = "service.http"
	httpSpec.Config = filterKeys(spec.Config, "base_url", "allowed_hosts", "timeout")
	if _, ok := httpSpec.Config["max_response_bytes"]; !ok {
		httpSpec.Config["max_response_bytes"] = 4 << 20
	}
	if bytes, ok := spec.Config["max_response_bytes"]; ok {
		httpSpec.Config["max_response_bytes"] = bytes
	}
	if _, ok := httpSpec.Config["timeout"]; !ok {
		httpSpec.Config["timeout"] = 60 * time.Second
	}
	client, _, err := openHTTPService(ctx, httpSpec)
	if err != nil {
		return nil, nil, err
	}

	return &llmService{
		http:           client.(*HTTPService),
		api:            api,
		model:          model,
		embeddingModel: configString(spec.Config, "embedding_model", ""),
		maxTokens:      maxTokens,
		apiKey:         apiKey,
		version:        configString(spec.Config, "anthropic_version", "2023-06-01"),
	}, nil, nil
}

// Chat implements spi.LLM.
func (l *llmService) Chat(ctx context.Context, request spi.ChatRequest) (spi.ChatResponse, error) {
	model := request.Model
	if model == "" {
		model = l.model
	}
	maxTokens := request.MaxTokens
	if maxTokens <= 0 {
		maxTokens = l.maxTokens
	}

	var (
		path    string
		payload map[string]any
		headers = map[string]string{"Content-Type": "application/json"}
	)
	if l.api == "anthropic" {
		path = "/v1/messages"
		headers["x-api-key"] = l.apiKey
		headers["anthropic-version"] = l.version
		messages := make([]map[string]any, 0, len(request.Messages))
		for _, message := range request.Messages {
			if message.Role == "system" {
				// Anthropic carries the system prompt out of band rather than as
				// a message, so a caller who put it in the list still gets the
				// behaviour they meant.
				if request.System == "" {
					request.System = message.Content
				}
				continue
			}
			messages = append(messages, map[string]any{"role": message.Role, "content": message.Content})
		}
		payload = map[string]any{"model": model, "messages": messages, "max_tokens": maxTokens}
		if request.System != "" {
			payload["system"] = request.System
		}
	} else {
		path = "/v1/chat/completions"
		headers["Authorization"] = "Bearer " + l.apiKey
		messages := make([]map[string]any, 0, len(request.Messages)+1)
		if request.System != "" {
			messages = append(messages, map[string]any{"role": "system", "content": request.System})
		}
		for _, message := range request.Messages {
			messages = append(messages, map[string]any{"role": message.Role, "content": message.Content})
		}
		payload = map[string]any{"model": model, "messages": messages, "max_tokens": maxTokens}
	}
	if request.Temperature != nil {
		payload["temperature"] = *request.Temperature
	}
	if len(request.Stop) > 0 {
		payload["stop"] = request.Stop
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return spi.ChatResponse{}, err
	}
	response, err := l.http.Do(ctx, HTTPRequest{Method: http.MethodPost, URL: path, Headers: headers, Body: body})
	if err != nil {
		return spi.ChatResponse{}, modelFailure(err, response)
	}
	return parseChatResponse(l.api, response, model)
}

// Embed implements spi.Embedder.
func (l *llmService) Embed(ctx context.Context, model string, inputs []string) ([][]float32, error) {
	if l.api != "openai" {
		return nil, fmt.Errorf("%w: embeddings are only implemented for the OpenAI request shape", spi.ErrUnsupported)
	}
	if model == "" {
		model = l.embeddingModel
	}
	if model == "" {
		return nil, fmt.Errorf("no embedding model configured: set embedding_model on the resource or model on the node")
	}
	body, err := json.Marshal(map[string]any{"model": model, "input": inputs})
	if err != nil {
		return nil, err
	}
	response, err := l.http.Do(ctx, HTTPRequest{
		Method:  http.MethodPost,
		URL:     "/v1/embeddings",
		Headers: map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + l.apiKey},
		Body:    body,
	})
	if err != nil {
		return nil, modelFailure(err, response)
	}
	var decoded struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.Unmarshal(response.Body, &decoded); err != nil {
		return nil, err
	}
	out := make([][]float32, len(decoded.Data))
	for i, item := range decoded.Data {
		out[i] = item.Embedding
	}
	return out, nil
}

func parseChatResponse(api string, response HTTPResponse, model string) (spi.ChatResponse, error) {
	result := spi.ChatResponse{Model: model}
	if api == "anthropic" {
		var decoded struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			StopReason string `json:"stop_reason"`
			Model      string `json:"model"`
			Usage      struct {
				InputTokens  int `json:"input_tokens"`
				OutputTokens int `json:"output_tokens"`
			} `json:"usage"`
		}
		if err := json.Unmarshal(response.Body, &decoded); err != nil {
			return result, err
		}
		var text strings.Builder
		for _, block := range decoded.Content {
			if block.Type == "text" {
				text.WriteString(block.Text)
			}
		}
		result.Text = text.String()
		result.FinishReason = decoded.StopReason
		result.InputTokens = decoded.Usage.InputTokens
		result.OutputTokens = decoded.Usage.OutputTokens
		if decoded.Model != "" {
			result.Model = decoded.Model
		}
		return result, nil
	}
	var decoded struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Model string `json:"model"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(response.Body, &decoded); err != nil {
		return result, err
	}
	if len(decoded.Choices) == 0 {
		return result, fmt.Errorf("the model returned no choices")
	}
	result.Text = decoded.Choices[0].Message.Content
	result.FinishReason = decoded.Choices[0].FinishReason
	result.InputTokens = decoded.Usage.PromptTokens
	result.OutputTokens = decoded.Usage.CompletionTokens
	if decoded.Model != "" {
		result.Model = decoded.Model
	}
	return result, nil
}

// modelFailure maps a provider's HTTP status onto a platform failure category,
// so a rate-limited model surfaces as a 429 to the caller rather than a 500 —
// and a retry policy can tell the difference.
func modelFailure(err error, response HTTPResponse) error {
	switch response.Status {
	case http.StatusTooManyRequests:
		return intent.Failure{Code: "RATE_LIMITED", Category: intent.CategoryRateLimit, Message: "the model provider is rate limiting this deployment", Cause: err}
	case http.StatusUnauthorized, http.StatusForbidden:
		return intent.Failure{Code: "UPSTREAM_AUTH", Category: intent.CategoryUnavailable, Message: "the model provider rejected our credentials", Cause: err}
	case http.StatusRequestEntityTooLarge:
		return intent.Failure{Code: "INVALID_INPUT", Category: intent.CategoryInvalidInput, Message: "the prompt is too large for this model", Cause: err}
	}
	return err
}

var (
	_ spi.Mailer   = (*smtpService)(nil)
	_ spi.LLM      = (*llmService)(nil)
	_ spi.Embedder = (*llmService)(nil)
)
