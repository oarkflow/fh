// Command files demonstrates uploads, safe persistence, inline file responses,
// downloads, ranges/conditional requests, and structured error responses.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
)

func main() {
	uploadDir := env("UPLOAD_DIR", "./uploads")
	if err := os.MkdirAll(uploadDir, 0700); err != nil {
		log.Fatal(err)
	}

	app := fh.New(fh.WithDebug(os.Getenv("DEBUG") == "1"))

	app.Get("/", func(c *fh.Ctx) error {
		return c.JSON(map[string]any{
			"upload":   "curl -F 'document=@./photo.jpg' -F 'title=My photo' http://localhost:3000/upload",
			"inline":   "curl http://localhost:3000/files/<stored-name>",
			"download": "curl -OJ http://localhost:3000/download/<stored-name>",
			"range":    "curl -H 'Range: bytes=0-99' http://localhost:3000/download/<stored-name>",
		})
	})

	app.Post("/upload", func(c *fh.Ctx) error {
		form, err := c.MultipartForm()
		if err != nil {
			return err
		}
		file, err := form.File("document")
		if err != nil {
			return &fh.ValidationError{Fields: []fh.FieldError{{Field: "document", Code: "REQUIRED", Message: "a file is required"}}}
		}

		// Never use a client filename as a path. Keep only its base name and
		// prefix it with a server-generated value to avoid collisions.
		original := filepath.Base(file.FileName)
		stored := strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + original
		destination := filepath.Join(uploadDir, stored)
		if err := c.SaveFile(file, destination); err != nil {
			return fh.WrapHTTPError(err, fh.StatusInternalServerError, "UPLOAD_SAVE_FAILED", "The upload could not be saved")
		}

		return c.Status(fh.StatusCreated).JSON(map[string]any{
			"title":    form.First("title"),
			"original": original,
			"stored":   stored,
			"size":     file.Size,
			"type":     file.Header.Get("Content-Type"),
		})
	})

	app.Get("/files/:name", func(c *fh.Ctx) error {
		path, err := storedPath(uploadDir, c.Param("name"))
		if err != nil {
			return err
		}
		return c.File(path)
	})

	app.Get("/download/:name", func(c *fh.Ctx) error {
		path, err := storedPath(uploadDir, c.Param("name"))
		if err != nil {
			return err
		}
		// A custom client-visible name can be passed as the second argument.
		return c.Download(path, strings.TrimPrefix(c.Param("name"), "download-"))
	})

	app.Get("/errors/conflict", func(*fh.Ctx) error {
		return fh.Conflict("A file with that version already exists")
	})

	app.Get("/errors/rate-limit", func(*fh.Ctx) error {
		return fh.RateLimited("Upload limit reached", "60")
	})

	app.Get("/errors/internal", func(*fh.Ctx) error {
		return errors.New("private database detail: connection refused")
	})

	log.Printf("file example listening on http://localhost:3000; uploads in %s", uploadDir)
	log.Fatal(app.Listen(":3000"))
}

func storedPath(root, name string) (string, error) {
	if name == "" || name == "." || filepath.Base(name) != name {
		return "", fh.BadRequest("Invalid file name")
	}
	filename := filepath.Join(root, name)
	if _, err := os.Stat(filename); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", fh.NotFound("File not found")
		}
		return "", fmt.Errorf("stat uploaded file: %w", err)
	}
	return filename, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
