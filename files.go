package fh

import (
	"errors"
	"io/fs"
	"mime"
	"os"
	"path/filepath"
	"strings"
)

// MultipartForm parses and caches the request's multipart form.
func (c *DefaultCtx) MultipartForm() (*MultipartForm, error) {
	if c.multipartParsed {
		return c.multipartForm, c.multipartErr
	}
	c.multipartParsed = true
	if !strings.HasPrefix(strings.ToLower(c.Get("Content-Type")), "multipart/form-data") {
		c.multipartErr = UnsupportedMediaType("expected multipart/form-data")
		return nil, c.multipartErr
	}
	var form MultipartForm
	if err := c.BodyParser(&form); err != nil {
		he := BadRequest("invalid multipart form")
		he.Err = err
		c.multipartErr = he
		return nil, c.multipartErr
	}
	c.multipartForm = &form
	return c.multipartForm, nil
}

// FormFile returns the first uploaded file for field.
func (c *DefaultCtx) FormFile(field string) (*MultipartFile, error) {
	form, err := c.MultipartForm()
	if err != nil {
		return nil, err
	}
	return form.File(field)
}

// SaveFile persists an uploaded file to an explicit destination.
func (c *DefaultCtx) SaveFile(file *MultipartFile, dst string) error {
	if file == nil {
		return errors.New("fasthttp: nil uploaded file")
	}
	return file.Save(dst)
}

func saveUploadedFile(dst string, data []byte, mode fs.FileMode) error {
	if dst == "" {
		return errors.New("fasthttp: empty file destination")
	}
	dir := filepath.Dir(dst)
	tmp, err := os.CreateTemp(dir, ".fasthttp-upload-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpName)
		}
	}()
	if err = tmp.Chmod(mode); err != nil {
		return err
	}
	if _, err = tmp.Write(data); err != nil {
		return err
	}
	if err = tmp.Sync(); err != nil {
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpName, dst); err != nil {
		return err
	}
	ok = true
	return nil
}

// Attachment marks a response as a download using a safely encoded filename.
func (c *DefaultCtx) Attachment(filename string) Ctx {
	name := filepath.Base(filename)
	if name == "." || name == string(filepath.Separator) {
		name = "download"
	}
	value := mime.FormatMediaType("attachment", map[string]string{"filename": name})
	c.Set("Content-Disposition", value)
	return c
}

// SendFile serves one file from disk with MIME detection, ETag,
// Last-Modified, conditional requests, and byte ranges.
func (c *DefaultCtx) SendFile(filename string) error {
	info, err := os.Stat(filename)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return NotFound("File not found")
	}
	abs, err := filepath.Abs(filename)
	if err != nil {
		return err
	}
	dir, base := filepath.Dir(abs), filepath.Base(abs)
	s := staticFS{fs: os.DirFS(dir), cfg: DefaultStaticConfig}
	return s.writeFile(c, base, info)
}

// File is an alias for SendFile.
func (c *DefaultCtx) File(filename string) error { return c.SendFile(filename) }

// Download serves a file as an attachment. The optional filename controls the
// client-visible name without changing the source path.
func (c *DefaultCtx) Download(filename string, downloadName ...string) error {
	name := filepath.Base(filename)
	if len(downloadName) > 0 && downloadName[0] != "" {
		name = downloadName[0]
	}
	c.Attachment(name)
	return c.SendFile(filename)
}
