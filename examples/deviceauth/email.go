package main

import (
	"crypto/rand"
	"crypto/tls"
	"fmt"
	"log"
	"math/big"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"strings"
)

type verificationMailer struct {
	host, port, username, password, from string
	console                              bool
}

func newVerificationCode() (string, error) {
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%06d", n.Int64()), nil
}

func newVerificationMailer() (*verificationMailer, error) {
	mailer := &verificationMailer{
		host:     strings.TrimSpace(os.Getenv("FH_DEVICEAUTH_SMTP_HOST")),
		port:     strings.TrimSpace(os.Getenv("FH_DEVICEAUTH_SMTP_PORT")),
		username: os.Getenv("FH_DEVICEAUTH_SMTP_USER"),
		password: os.Getenv("FH_DEVICEAUTH_SMTP_PASSWORD"),
		from:     strings.TrimSpace(os.Getenv("FH_DEVICEAUTH_EMAIL_FROM")),
	}
	if mailer.host == "" {
		mailer.console = true // The server is loopback-only; this is convenient for the local demo.
		return mailer, nil
	}
	if mailer.port == "" || mailer.username == "" || mailer.password == "" {
		return nil, fmt.Errorf("configure FH_DEVICEAUTH_SMTP_PORT, FH_DEVICEAUTH_SMTP_USER, and FH_DEVICEAUTH_SMTP_PASSWORD")
	}
	from, err := mail.ParseAddress(mailer.from)
	if err != nil || !strings.EqualFold(from.Address, mailer.from) {
		return nil, fmt.Errorf("FH_DEVICEAUTH_EMAIL_FROM must be a valid email address")
	}
	return mailer, nil
}

func (m *verificationMailer) send(to, code string) error {
	if m.console {
		log.Printf("DEV ONLY: email verification code for %s: %s", to, code)
		return nil
	}
	address := net.JoinHostPort(m.host, m.port)
	client, err := smtp.Dial(address)
	if err != nil {
		return err
	}
	defer client.Close()
	if ok, _ := client.Extension("STARTTLS"); !ok {
		return fmt.Errorf("SMTP server does not offer STARTTLS")
	}
	if err := client.StartTLS(&tls.Config{MinVersion: tls.VersionTLS12, ServerName: m.host}); err != nil {
		return err
	}
	if err := client.Auth(smtp.PlainAuth("", m.username, m.password, m.host)); err != nil {
		return err
	}
	if err := client.Mail(m.from); err != nil {
		return err
	}
	if err := client.Rcpt(to); err != nil {
		return err
	}
	writer, err := client.Data()
	if err != nil {
		return err
	}
	body := fmt.Sprintf("From: %s\r\nTo: %s\r\nSubject: Verify your FH Device Auth registration\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\nYour registration verification code is %s. It expires in 10 minutes.\r\n", m.from, to, code)
	if _, err := writer.Write([]byte(body)); err != nil {
		_ = writer.Close()
		return err
	}
	return writer.Close()
}
