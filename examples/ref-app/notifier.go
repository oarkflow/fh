package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/smtp"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

// Notifiers: the one place this application talks to the outside world.
//
// Three implementations, chosen by configuration, and the choice is explicit: there
// is no "try SMTP and quietly fall back to printing". A deployment that has not
// configured delivery is running with REF_APP_NOTIFIER=stdout and can see that in
// its own startup log.

// Notifier delivers one notification. An error means "not delivered" and the outbox
// will retry, so an implementation must not report success on a partial send.
type Notifier interface {
	Notify(ctx context.Context, notification Notification) error
	Describe() string
}

func NewNotifier(cfg Config, log *slog.Logger) (Notifier, error) {
	switch cfg.Notifier {
	case "smtp":
		return &SMTPNotifier{
			addr:     net.JoinHostPort(cfg.SMTPHost, fmt.Sprint(cfg.SMTPPort)),
			from:     cfg.SMTPFrom,
			username: cfg.SMTPUser,
			password: cfg.SMTPPassword,
		}, nil
	case "webhook":
		parsed, err := url.Parse(cfg.WebhookURL)
		if err != nil {
			return nil, fmt.Errorf("ref-app: REF_APP_WEBHOOK_URL is not a URL: %w", err)
		}
		if parsed.Scheme != "http" && parsed.Scheme != "https" {
			return nil, fmt.Errorf("ref-app: REF_APP_WEBHOOK_URL must be http or https")
		}
		if !slices.Contains(cfg.WebhookHosts, parsed.Hostname()) {
			// The allowlist is checked here, at startup, as well as per request:
			// a misconfiguration should not wait for the first notification.
			return nil, fmt.Errorf("ref-app: %q is not in REF_APP_WEBHOOK_HOSTS", parsed.Hostname())
		}
		return &WebhookNotifier{
			url:     cfg.WebhookURL,
			allowed: cfg.WebhookHosts,
			client:  fh.NewClient(fh.ClientConfig{Timeout: 10 * time.Second, UserAgent: "ref-app/1"}),
		}, nil
	default:
		return &StdoutNotifier{log: log}, nil
	}
}

// ---------------------------------------------------------------------------

// StdoutNotifier prints. It is honest about being a development stand-in — it says
// so in Describe(), which the startup log prints — rather than being a "mail
// service" that never mails.
type StdoutNotifier struct{ log *slog.Logger }

func (n *StdoutNotifier) Describe() string {
	return "stdout (development: notifications are printed, not delivered)"
}

func (n *StdoutNotifier) Notify(_ context.Context, notification Notification) error {
	n.log.Info("notification",
		slog.String("channel", notification.Channel),
		slog.String("to", notification.To),
		slog.String("subject", notification.Subject),
		slog.String("body", notification.Body))
	return nil
}

// ---------------------------------------------------------------------------

// SMTPNotifier sends real mail over SMTP, upgrading to TLS when the server offers
// it and authenticating only over an encrypted connection.
type SMTPNotifier struct {
	addr     string
	from     string
	username string
	password string
}

func (n *SMTPNotifier) Describe() string { return "smtp " + n.addr }

func (n *SMTPNotifier) Notify(ctx context.Context, notification Notification) error {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(15 * time.Second)
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", n.addr)
	if err != nil {
		return err
	}
	_ = conn.SetDeadline(deadline)

	host, _, _ := net.SplitHostPort(n.addr)
	client, err := smtp.NewClient(conn, host)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer func() { _ = client.Quit() }()

	if ok, _ := client.Extension("STARTTLS"); ok {
		if err := client.StartTLS(nil); err != nil {
			return err
		}
	}
	if n.username != "" {
		// Credentials go only over an encrypted connection. Sending them in the
		// clear because the server did not offer STARTTLS is not a fallback, it is
		// a leak.
		if ok, _ := client.Extension("STARTTLS"); !ok {
			return fmt.Errorf("refusing to send SMTP credentials over an unencrypted connection")
		}
		if err := client.Auth(smtp.PlainAuth("", n.username, n.password, host)); err != nil {
			return err
		}
	}
	if err := client.Mail(n.from); err != nil {
		return err
	}
	if err := client.Rcpt(notification.To); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	var message bytes.Buffer
	message.WriteString("From: ")
	message.WriteString(n.from)
	message.WriteString("\r\nTo: ")
	message.WriteString(notification.To)
	message.WriteString("\r\nSubject: ")
	message.WriteString(sanitizeHeader(notification.Subject))
	message.WriteString("\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n")
	message.WriteString(notification.Body)
	if _, err := writer.Write(message.Bytes()); err != nil {
		return err
	}
	return writer.Close()
}

// sanitizeHeader strips CR and LF. A newline in a header value is a header
// injection, and a subject line is attacker-influenced in almost every application.
func sanitizeHeader(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(value)
}

// ---------------------------------------------------------------------------

// WebhookNotifier posts the notification as JSON to one allowlisted host.
type WebhookNotifier struct {
	url     string
	allowed []string
	client  *fh.Client
}

func (n *WebhookNotifier) Describe() string { return "webhook " + n.url }

func (n *WebhookNotifier) Notify(ctx context.Context, notification Notification) error {
	parsed, err := url.Parse(n.url)
	if err != nil {
		return err
	}
	// Re-checked per request, not only at startup: the allowlist is the control that
	// keeps a configuration mistake from turning into an outbound request to an
	// arbitrary host.
	if !slices.Contains(n.allowed, parsed.Hostname()) {
		return fmt.Errorf("host %q is not allowlisted", parsed.Hostname())
	}
	payload, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	response, err := n.client.R().
		Header("Content-Type", "application/json").
		Body(payload).
		Post(ctx, n.url)
	if err != nil {
		return err
	}
	// The body is read and discarded so the connection can be reused; an endpoint
	// that answers with a large body should not hold a connection open either.
	_, _ = response.Bytes()
	if status := response.StatusCode(); status >= 300 {
		return fmt.Errorf("the notification endpoint answered %d", status)
	}
	return nil
}
