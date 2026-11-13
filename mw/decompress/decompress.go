// Package decompress provides bounded request decompression.
package decompress

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/oarkflow/fh"
)

var (
	ErrTooLarge         = errors.New("decompress: expanded body exceeds limit")
	ErrExpansionRatio   = errors.New("decompress: expansion ratio exceeds limit")
	ErrUnsupported      = errors.New("decompress: unsupported content encoding")
	ErrMalformedContent = errors.New("decompress: malformed compressed content")
)

type Config struct {
	MaxSize           int
	MaxExpansionRatio int
	Next              func(fh.Ctx) bool
}

func New(config ...Config) fh.HandlerFunc {
	cfg := Config{MaxSize: 4 << 20, MaxExpansionRatio: 100}
	if len(config) > 0 {
		if config[0].MaxSize > 0 {
			cfg.MaxSize = config[0].MaxSize
		}
		if config[0].MaxExpansionRatio > 0 {
			cfg.MaxExpansionRatio = config[0].MaxExpansionRatio
		}
		cfg.Next = config[0].Next
	}
	return func(c fh.Ctx) error {
		if cfg.Next != nil && cfg.Next(c) {
			return c.Next()
		}
		encoding := strings.ToLower(strings.TrimSpace(c.Get(fh.HeaderContentEncodingStr)))
		if encoding == "" || encoding == "identity" {
			return c.Next()
		}
		if encoding != "gzip" {
			return fh.NewHTTPError(fh.StatusUnsupportedMediaType, "CONTENT_ENCODING_UNSUPPORTED", "Content-Encoding is not supported").WithCause(ErrUnsupported)
		}
		requestBody := c.Body()
		if err := fh.RequestBodyLimitError(c); err != nil {
			return requestBodyError(err)
		}
		gz, err := gzip.NewReader(bytes.NewReader(requestBody))
		if err != nil {
			return compressedBodyError(err)
		}
		limit, ratioLimited := expandedLimit(len(requestBody), cfg.MaxSize, cfg.MaxExpansionRatio)
		var body []byte
		_, defaultCtx := c.(*fh.DefaultCtx)
		if !defaultCtx || !fh.ReplaceRequestBodyReader(c, io.LimitReader(gz, int64(limit)+1)) {
			_ = gz.Close()
			body, err = DecodeGzip(requestBody, cfg.MaxSize, cfg.MaxExpansionRatio)
			if err != nil {
				return compressedBodyError(err)
			}
			if !fh.ReplaceRequestBody(c, body) {
				if err := fh.RequestBodyLimitError(c); err != nil {
					return requestBodyError(err)
				}
				return fh.NewHTTPError(fh.StatusInternalServerError, "REQUEST_BODY_REPLACE_UNSUPPORTED", "Request context cannot replace its body")
			}
		} else {
			body = c.Body()
			readErr := fh.RequestBodyLimitError(c)
			_ = gz.Close()
			if readErr != nil {
				return requestBodyError(readErr)
			}
			if len(body) > limit {
				decodeErr := ErrTooLarge
				if ratioLimited {
					decodeErr = ErrExpansionRatio
				}
				return compressedBodyError(decodeErr)
			}
		}
		c.RequestHeader().Del(fh.HeaderContentEncodingStr)
		c.RequestHeader().Del(fh.HeaderContentLengthStr)
		c.RequestHeader().ContentLength = len(body)
		c.RequestHeader().HasContentLength = true
		return c.Next()
	}
}

func expandedLimit(compressedSize, maxSize, maxRatio int) (limit int, ratioLimited bool) {
	limit = maxSize
	if maxRatio > 0 && compressedSize > 0 && compressedSize <= maxSize/maxRatio {
		ratioLimit := compressedSize * maxRatio
		if ratioLimit < limit {
			limit = ratioLimit
			ratioLimited = true
		}
	}
	return limit, ratioLimited
}

func requestBodyError(err error) error {
	if errors.Is(err, fh.ErrBufferedBodyBudget) {
		return fh.NewHTTPError(fh.StatusServiceUnavailable, "REQUEST_BODY_BUDGET_EXCEEDED", "Request body buffer capacity is exhausted").WithCause(err).Temporary()
	}
	if errors.Is(err, fh.ErrBodyTooLarge) {
		return fh.PayloadTooLarge("Decompressed request body exceeds the configured limit").WithCause(err)
	}
	return compressedBodyError(fmt.Errorf("%w: %v", ErrMalformedContent, err))
}

func compressedBodyError(err error) error {
	status := fh.StatusBadRequest
	code := "COMPRESSED_BODY_INVALID"
	if errors.Is(err, ErrTooLarge) || errors.Is(err, ErrExpansionRatio) {
		status = fh.StatusPayloadTooLarge
		code = "DECOMPRESSED_BODY_TOO_LARGE"
	}
	return fh.NewHTTPError(status, code, "Compressed request body was rejected").WithCause(err)
}

// DecodeGzip expands src with both an absolute limit and an expansion-ratio
// limit. The ratio check makes small, highly compressible bombs fail early.
func DecodeGzip(src []byte, maxSize, maxRatio int) ([]byte, error) {
	if maxSize <= 0 {
		return nil, ErrTooLarge
	}
	r, err := gzip.NewReader(bytes.NewReader(src))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedContent, err)
	}
	defer r.Close()
	limit := maxSize
	if maxRatio > 0 && len(src) > 0 && len(src) <= maxSize/maxRatio {
		ratioLimit := len(src) * maxRatio
		if ratioLimit < limit {
			limit = ratioLimit
		}
	}
	out, err := io.ReadAll(io.LimitReader(r, int64(limit)+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformedContent, err)
	}
	if len(out) > maxSize {
		return nil, ErrTooLarge
	}
	if maxRatio > 0 && len(src) > 0 && maxRatio <= maxSize/len(src) && len(out) > len(src)*maxRatio {
		return nil, ErrExpansionRatio
	}
	return out, nil
}
