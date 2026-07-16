package proxy

import (
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"errors"
	"hash"
	"io"
	"net/http"
	"strings"

	"github.com/oarkflow/fh"
)

var ErrIntegrityMismatch = errors.New("proxy: subresource integrity check failed")

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

	h := sha256.New()
	limited := io.LimitReader(resp.Body, maxBody+1)
	n, err := io.Copy(h, limited)
	if err != nil {
		return err
	}
	if n > maxBody {
		resp.Body.Close()
		return errors.New("proxy: response body exceeds SRI verification limit")
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
			actual = h.(hash.Hash).Sum(nil)
		case "sha384":
			h384 := sha512.New384()
			h384.Write(h.(hash.Hash).Sum(nil))
			actual = h384.Sum(nil)
		case "sha512":
			h512 := sha512.New()
			h512.Write(h.(hash.Hash).Sum(nil))
			actual = h512.Sum(nil)
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
