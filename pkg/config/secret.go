package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const maxSecretFileSize = 64 << 10

// SecretString resolves one secret at startup. A mounted secret file is
// preferred because environment values may be exposed through process
// inspection, crash reports, and child-process inheritance. Direct environment
// values remain supported for compatibility, but configuring both sources is
// rejected so key rotation and deployment mistakes fail closed.
//
// SecretString does not cache values or watch files. Call it during startup and
// retain the decoded key/credential in the component that consumes it.
func SecretString(valueEnv, fileEnv string) (string, error) {
	value := os.Getenv(valueEnv)
	path := strings.TrimSpace(os.Getenv(fileEnv))
	if value != "" && path != "" {
		return "", fmt.Errorf("config: %s and %s cannot both be set", valueEnv, fileEnv)
	}
	if path == "" {
		return value, nil
	}

	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("config: open secret file from %s: %w", fileEnv, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("config: stat secret file from %s: %w", fileEnv, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("config: secret file from %s is not a regular file", fileEnv)
	}

	b, err := io.ReadAll(io.LimitReader(f, maxSecretFileSize+1))
	if err != nil {
		return "", fmt.Errorf("config: read secret file from %s: %w", fileEnv, err)
	}
	if len(b) > maxSecretFileSize {
		return "", fmt.Errorf("config: secret file from %s exceeds %d bytes", fileEnv, maxSecretFileSize)
	}
	// Secret volume files conventionally end with one line ending. Remove only
	// that delimiter; leading/trailing spaces may be intentional credentials.
	secret := strings.TrimSuffix(string(b), "\n")
	secret = strings.TrimSuffix(secret, "\r")
	if secret == "" {
		return "", fmt.Errorf("config: secret file from %s is empty", fileEnv)
	}
	return secret, nil
}

// RequireSecretString resolves a secret and fails when neither source is
// configured. It is useful for production-only credentials and private keys.
func RequireSecretString(valueEnv, fileEnv string) (string, error) {
	value, err := SecretString(valueEnv, fileEnv)
	if err != nil {
		return "", err
	}
	if value == "" {
		return "", errors.New("config: secret is required; set " + fileEnv + " to a mounted secret file")
	}
	return value, nil
}
