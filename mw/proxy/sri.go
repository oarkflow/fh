package proxy

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/oarkflow/fh"
)

var ErrIntegrityMismatch = errors.New("proxy: subresource integrity check failed")
var ErrBodyTooLarge = errors.New("proxy: response body exceeds SRI verification limit")

type SRIConfig struct {
	Required    bool
	Integrities map[string]string
	MaxBodySize int64
}

func VerifyIntegrity(resp *http.Response, expectedHash string, maxBody int64) error {
	if expectedHash == "" {
		return nil
	}
	if maxBody <= 0 {
		maxBody = 10 << 20
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBody+1))
	resp.Body.Close()
	if err != nil {
		return err
	}
	if int64(len(body)) > maxBody {
		return ErrBodyTooLarge
	}

	algorithms := strings.Split(expectedHash, " ")
	for _, algoHash := range algorithms {
		parts := strings.SplitN(algoHash, "-", 2)
		if len(parts) != 2 {
			continue
		}
		algo := parts[0]
		expected := parts[1]

		decoded, err := base64.StdEncoding.DecodeString(expected)
		if err != nil {
			continue
		}

		var actual []byte
		switch algo {
		case "sha256":
			h := sha256.Sum256(body)
			actual = h[:]
		case "sha384":
			h := sha512.Sum384(body)
			actual = h[:]
		case "sha512":
			h := sha512.Sum512(body)
			actual = h[:]
		default:
			continue
		}

		if base64.StdEncoding.EncodeToString(actual) == base64.StdEncoding.EncodeToString(decoded) {
			return nil
		}
	}

	return ErrIntegrityMismatch
}

func WithSRI(cfg SRIConfig) func(fh.Ctx, *http.Response) error {
	if cfg.MaxBodySize <= 0 {
		cfg.MaxBodySize = 10 << 20
	}
	return func(c fh.Ctx, resp *http.Response) error {
		path := c.Path()
		if expected, ok := cfg.Integrities[path]; ok {
			if err := VerifyIntegrity(resp, expected, cfg.MaxBodySize); err != nil {
				if cfg.Required {
					return err
				}
				c.Set("X-SRI-Warning", "integrity mismatch")
			}
		}
		return nil
	}
}
