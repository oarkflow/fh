package fh

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
)

// MultipartStreamOptions bounds incremental multipart parsing. Zero values use
// the configured codec limits; MaxTotalBytes defaults to the server body cap.
type MultipartStreamOptions struct {
	MaxParts      int
	MaxFieldBytes int64
	MaxFileBytes  int64
	MaxTotalBytes int64
}

// MultipartStreamPart is valid only while the StreamMultipart callback runs.
// Reader yields one part without retaining its contents in memory.
type MultipartStreamPart struct {
	FieldName string
	FileName  string
	Header    map[string][]string
	Reader    io.Reader
}

// StreamMultipart visits each form part without buffering its contents when
// the application enables WithStreamRequestBody(true). A callback may consume
// part.Reader or leave it unread; the remaining bytes are drained with the
// same per-part limit before the next part is visited.
func StreamMultipart(c Ctx, options MultipartStreamOptions, visit func(MultipartStreamPart) error) error {
	if visit == nil {
		return errors.New("fh: nil multipart callback")
	}
	mediaType, params, err := mime.ParseMediaType(c.Get(HeaderContentType))
	if err != nil || mediaType != "multipart/form-data" || params["boundary"] == "" {
		return UnsupportedMediaType("expected multipart/form-data with boundary")
	}
	defaults := getCodecOptions()
	if options.MaxParts <= 0 {
		options.MaxParts = defaults.MaxMultipartParts
	}
	if options.MaxFieldBytes <= 0 {
		options.MaxFieldBytes = defaults.MaxMultipartFieldSize
	}
	if options.MaxFileBytes <= 0 {
		options.MaxFileBytes = defaults.MaxMultipartFileSize
	}
	if options.MaxTotalBytes <= 0 {
		if dc, ok := c.(*DefaultCtx); ok && dc.server != nil {
			options.MaxTotalBytes = int64(dc.server.cfg.MaxRequestBodySize)
		}
	}
	return c.StreamBody(func(body io.Reader) error {
		reader := io.Reader(&contextRequestReader{ctx: c.Context(), reader: body})
		var total *boundedRequestReader
		if options.MaxTotalBytes > 0 {
			total = &boundedRequestReader{reader: reader, left: options.MaxTotalBytes}
			reader = total
		}
		multi := multipart.NewReader(reader, params["boundary"])
		for count := 0; ; count++ {
			if err := c.Context().Err(); err != nil {
				return err
			}
			part, err := multi.NextPart()
			if total != nil && total.err != nil {
				return total.err
			}
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return fmt.Errorf("multipart: %w", err)
			}
			if count >= options.MaxParts {
				_ = part.Close()
				return PayloadTooLarge("multipart part count exceeds configured limit")
			}
			limit := options.MaxFieldBytes
			if part.FileName() != "" {
				limit = options.MaxFileBytes
			}
			bounded := &boundedRequestReader{reader: part, left: limit}
			item := MultipartStreamPart{FieldName: part.FormName(), FileName: part.FileName(), Header: part.Header, Reader: bounded}
			visitErr := visit(item)
			if visitErr == nil {
				_, visitErr = io.Copy(io.Discard, bounded)
			}
			_ = part.Close()
			if bounded.err != nil {
				return bounded.err
			}
			if total != nil && total.err != nil {
				return total.err
			}
			if visitErr != nil {
				return visitErr
			}
		}
	})
}

type contextRequestReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextRequestReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}
