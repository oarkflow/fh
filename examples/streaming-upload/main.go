package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"io"
	"log"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mw/bodylimit"
	"github.com/oarkflow/fh/mw/contract"
	"github.com/oarkflow/fh/mw/metrics"
)

const (
	globalBodyLimit = 8 << 20
	uploadBodyLimit = 6 << 20
	fileLimit       = 5 << 20
)

func buildApp() *fh.App {
	app := fh.New(
		fh.WithStreamRequestBody(true),
		fh.WithMaxRequestBodySize(globalBodyLimit),
	)
	m := metrics.NewWithConfig(metrics.Config{MaxRoutes: 32})
	app.Use(m.Middleware())
	app.Use(bodylimit.New(globalBodyLimit))

	app.Post("/upload", contract.New(contract.Config{
		Methods:      []string{"POST"},
		ContentTypes: []string{"multipart/form-data"},
		MaxBodyBytes: uploadBodyLimit,
	}), upload)
	app.Get("/metrics", m.Handler())

	// The companion bounded-download example uses this predictable payload.
	sample := []byte(strings.Repeat("fh sample response\n", 4096))
	app.Get("/sample", func(c fh.Ctx) error {
		c.Type("application/octet-stream")
		return c.SendStreamLength(bytes.NewReader(sample), int64(len(sample)))
	})
	return app
}

func upload(c fh.Ctx) error {
	var title, filename, digest string
	var size int64
	foundFile := false
	err := fh.StreamMultipart(c, fh.MultipartStreamOptions{
		MaxParts:      3,
		MaxFieldBytes: 4 << 10,
		MaxFileBytes:  fileLimit,
		MaxTotalBytes: uploadBodyLimit,
	}, func(part fh.MultipartStreamPart) error {
		switch part.FieldName {
		case "title":
			value, err := io.ReadAll(part.Reader)
			if err != nil {
				return err
			}
			title = string(value)
		case "file":
			if foundFile || part.FileName == "" {
				return fh.BadRequest("expected one file part")
			}
			foundFile = true
			filename = part.FileName
			hash := sha256.New()
			var err error
			size, err = io.Copy(hash, part.Reader)
			if err != nil {
				return err
			}
			digest = hex.EncodeToString(hash.Sum(nil))
		default:
			return fh.BadRequest("unexpected multipart field")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !foundFile {
		return fh.BadRequest("file part is required")
	}
	return c.JSON(fh.Map{"title": title, "filename": filename, "bytes": size, "sha256": digest})
}

func main() {
	listen := flag.String("listen", "127.0.0.1:8085", "local listen address")
	flag.Parse()
	if err := buildApp().ListenWithGracefulShutdown(*listen); err != nil {
		log.Fatal(err)
	}
}
