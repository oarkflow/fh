package platform

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/platform/spi"
)

// Object storage providers.
//
// storage.fs writes to a directory; storage.sql writes to a table. Both exist
// because an application that only needs to keep a few thousand generated
// invoices should not have to stand up an object store first, and because a
// deployment that already has a database has somewhere durable and backed up to
// put them.
//
// The whole surface area of a filesystem-backed object store is key handling,
// so that is where the care goes: a key is validated and joined, the result is
// verified to still be inside the root, and symlinks are refused. "../" in a
// user-supplied filename is the oldest bug in this shape of code.

func registerStorageResources(r *Registry) {
	mustResource(r, "storage.fs", ResourceFactoryFunc(openFSStorage), ResourceKindInfo{
		Family:   "storage",
		Summary:  "Object storage in a directory. Keys are validated so nothing can be written outside the root.",
		Provides: []string{"ObjectStore"},
		Config: []ConfigField{
			{Name: "dir", Type: "string", Required: true},
			{Name: "max_object_bytes", Type: "int", Default: "26214400"},
			{Name: "file_mode", Type: "int", Default: "384", Summary: "Octal 0600 by default: readable only by the service user"},
		},
	})

	mustResource(r, "storage.sql", ResourceFactoryFunc(openSQLStorage), ResourceKindInfo{
		Family:   "storage",
		Summary:  "Object storage in a SQL table, so blobs are covered by the same backup and replication as the data referencing them.",
		Provides: []string{"ObjectStore"},
		Config: []ConfigField{
			{Name: "database", Type: "resource", Required: true},
			{Name: "table", Type: "string", Default: "platform_objects"},
			{Name: "migrate", Type: "bool", Default: "true"},
			{Name: "max_object_bytes", Type: "int", Default: "8388608", Summary: "Smaller than the filesystem default: large blobs belong outside a row"},
		},
	})
}

// ---------------------------------------------------------------------------
// Filesystem
// ---------------------------------------------------------------------------

type fsStorage struct {
	root     string
	maxBytes int64
	fileMode os.FileMode
}

func openFSStorage(_ context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("storage.fs", spec.Config, "dir", "max_object_bytes", "file_mode"); err != nil {
		return nil, nil, err
	}
	dir, err := requiredString(spec.Config, "dir")
	if err != nil {
		return nil, nil, fmt.Errorf("resource %q: %w", spec.Name, err)
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return nil, nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, nil, fmt.Errorf("resource %q: create storage directory: %w", spec.Name, err)
	}
	maxBytes, err := configInt64(spec.Config, "max_object_bytes", 25<<20)
	if err != nil {
		return nil, nil, err
	}
	mode, err := configInt(spec.Config, "file_mode", 0o600)
	if err != nil {
		return nil, nil, err
	}
	return &fsStorage{root: root, maxBytes: maxBytes, fileMode: os.FileMode(mode)}, nil, nil
}

// path validates a key and resolves it inside the root.
//
// Three checks, each catching a different mistake: the cleaned key must not
// escape upward, the joined path must still be prefixed by the root after
// symlink evaluation, and absolute keys are rejected outright.
func (s *fsStorage) path(key string) (string, error) {
	if key == "" {
		return "", errors.New("an object key is required")
	}
	if filepath.IsAbs(key) || strings.HasPrefix(key, "/") || strings.HasPrefix(key, `\`) {
		return "", fmt.Errorf("object key %q must be relative", key)
	}
	cleaned := filepath.Clean(filepath.FromSlash(key))
	if cleaned == "." || strings.HasPrefix(cleaned, "..") {
		return "", fmt.Errorf("object key %q escapes the storage root", key)
	}
	full := filepath.Join(s.root, cleaned)
	if !strings.HasPrefix(full, s.root+string(os.PathSeparator)) {
		return "", fmt.Errorf("object key %q escapes the storage root", key)
	}
	// Resolve symlinks on the parent directory, which is the part an attacker
	// could have pointed elsewhere. The leaf may legitimately not exist yet.
	if resolved, err := filepath.EvalSymlinks(filepath.Dir(full)); err == nil {
		if resolved != s.root && !strings.HasPrefix(resolved, s.root+string(os.PathSeparator)) {
			return "", fmt.Errorf("object key %q resolves outside the storage root", key)
		}
	}
	return full, nil
}

// Put implements spi.ObjectStore. The write is atomic: content goes to a
// temporary file in the same directory and is renamed into place, so a reader
// never sees a half-written object and a crash leaves no corrupt one.
func (s *fsStorage) Put(_ context.Context, key string, body io.Reader, contentType string) (spi.ObjectInfo, error) {
	full, err := s.path(key)
	if err != nil {
		return spi.ObjectInfo{}, err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return spi.ObjectInfo{}, err
	}
	temp, err := os.CreateTemp(filepath.Dir(full), ".tmp-*")
	if err != nil {
		return spi.ObjectInfo{}, err
	}
	tempName := temp.Name()
	defer func() {
		temp.Close()
		os.Remove(tempName)
	}()

	digest := sha256.New()
	written, err := io.Copy(io.MultiWriter(temp, digest), io.LimitReader(body, s.maxBytes+1))
	if err != nil {
		return spi.ObjectInfo{}, err
	}
	if written > s.maxBytes {
		return spi.ObjectInfo{}, fmt.Errorf("object exceeds the %d byte limit configured for this store", s.maxBytes)
	}
	if err := temp.Chmod(s.fileMode); err != nil {
		return spi.ObjectInfo{}, err
	}
	if err := temp.Sync(); err != nil {
		return spi.ObjectInfo{}, err
	}
	if err := temp.Close(); err != nil {
		return spi.ObjectInfo{}, err
	}
	if err := os.Rename(tempName, full); err != nil {
		return spi.ObjectInfo{}, err
	}
	return spi.ObjectInfo{
		Key:         key,
		Size:        written,
		ContentType: contentType,
		ETag:        hex.EncodeToString(digest.Sum(nil)),
		ModifiedAt:  nowUTC(),
	}, nil
}

// Get implements spi.ObjectStore.
func (s *fsStorage) Get(_ context.Context, key string) (io.ReadCloser, spi.ObjectInfo, error) {
	full, err := s.path(key)
	if err != nil {
		return nil, spi.ObjectInfo{}, err
	}
	info, err := os.Stat(full)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, spi.ObjectInfo{}, notFound("object", key)
		}
		return nil, spi.ObjectInfo{}, err
	}
	file, err := os.Open(full)
	if err != nil {
		return nil, spi.ObjectInfo{}, err
	}
	return file, spi.ObjectInfo{Key: key, Size: info.Size(), ModifiedAt: info.ModTime().UTC()}, nil
}

// Delete implements spi.ObjectStore. Deleting a missing object succeeds, so a
// retried cleanup does not fail.
func (s *fsStorage) Delete(_ context.Context, key string) error {
	full, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(full); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// List implements spi.ObjectStore.
func (s *fsStorage) List(_ context.Context, prefix string, limit int) ([]spi.ObjectInfo, error) {
	if limit <= 0 {
		limit = 1000
	}
	var out []spi.ObjectInfo
	err := filepath.WalkDir(s.root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || len(out) >= limit {
			return err
		}
		relative, relErr := filepath.Rel(s.root, path)
		if relErr != nil {
			return nil
		}
		key := filepath.ToSlash(relative)
		if strings.HasPrefix(filepath.Base(path), ".tmp-") || !strings.HasPrefix(key, prefix) {
			return nil
		}
		info, infoErr := entry.Info()
		if infoErr != nil {
			return nil
		}
		out = append(out, spi.ObjectInfo{Key: key, Size: info.Size(), ModifiedAt: info.ModTime().UTC()})
		return nil
	})
	return out, err
}

// ---------------------------------------------------------------------------
// SQL
// ---------------------------------------------------------------------------

type sqlStorage struct {
	db       *Database
	table    string
	maxBytes int64
}

func openSQLStorage(ctx context.Context, spec ResourceSpec) (Resource, io.Closer, error) {
	if err := rejectUnknownConfig("storage.sql", spec.Config, "database", "table", "migrate", "max_object_bytes"); err != nil {
		return nil, nil, err
	}
	db, err := requireSQLHandle(spec, "database")
	if err != nil {
		return nil, nil, err
	}
	table, err := safeIdentifier(configString(spec.Config, "table", "platform_objects"))
	if err != nil {
		return nil, nil, fmt.Errorf("storage.sql %q: %w", spec.Name, err)
	}
	maxBytes, err := configInt64(spec.Config, "max_object_bytes", 8<<20)
	if err != nil {
		return nil, nil, err
	}
	store := &sqlStorage{db: db, table: table, maxBytes: maxBytes}
	if configBool(spec.Config, "migrate", true) {
		if err := store.migrate(ctx); err != nil {
			return nil, nil, fmt.Errorf("storage.sql %q: %w", spec.Name, err)
		}
	}
	return store, nil, nil
}

func (s *sqlStorage) query(statement string) string { return rebind(s.db.Dialect, statement) }

func (s *sqlStorage) migrate(ctx context.Context) error {
	dialect := s.db.Dialect
	statement := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		object_key   %s NOT NULL PRIMARY KEY,
		content      %s,
		content_type %s,
		size_bytes   BIGINT NOT NULL,
		etag         %s,
		modified_at  %s NOT NULL
	)`, s.table, textType(dialect), blobType(dialect), textType(dialect), textType(dialect), timestampType(dialect))
	_, err := s.db.ExecContext(ctx, statement)
	return err
}

// Put implements spi.ObjectStore.
func (s *sqlStorage) Put(ctx context.Context, key string, body io.Reader, contentType string) (spi.ObjectInfo, error) {
	if key == "" {
		return spi.ObjectInfo{}, errors.New("an object key is required")
	}
	var buffer bytes.Buffer
	written, err := io.Copy(&buffer, io.LimitReader(body, s.maxBytes+1))
	if err != nil {
		return spi.ObjectInfo{}, err
	}
	if written > s.maxBytes {
		return spi.ObjectInfo{}, fmt.Errorf("object exceeds the %d byte limit configured for this store", s.maxBytes)
	}
	digest := sha256.Sum256(buffer.Bytes())
	etag := hex.EncodeToString(digest[:])
	modified := nowUTC()

	statement := fmt.Sprintf(`INSERT INTO %s (object_key, content, content_type, size_bytes, etag, modified_at)
		VALUES ($1,$2,$3,$4,$5,$6)`, s.table)
	if s.db.Dialect == "mysql" {
		statement += upsertClause(s.db.Dialect, "object_key",
			"content = VALUES(content)", "content_type = VALUES(content_type)",
			"size_bytes = VALUES(size_bytes)", "etag = VALUES(etag)", "modified_at = VALUES(modified_at)")
	} else {
		statement += upsertClause(s.db.Dialect, "object_key",
			"content = EXCLUDED.content", "content_type = EXCLUDED.content_type",
			"size_bytes = EXCLUDED.size_bytes", "etag = EXCLUDED.etag", "modified_at = EXCLUDED.modified_at")
	}
	if _, err := s.db.ExecContext(ctx, s.query(statement), key, buffer.Bytes(), contentType, written, etag, modified); err != nil {
		return spi.ObjectInfo{}, err
	}
	return spi.ObjectInfo{Key: key, Size: written, ContentType: contentType, ETag: etag, ModifiedAt: modified}, nil
}

// Get implements spi.ObjectStore.
func (s *sqlStorage) Get(ctx context.Context, key string) (io.ReadCloser, spi.ObjectInfo, error) {
	statement := fmt.Sprintf("SELECT content, content_type, size_bytes, etag, modified_at FROM %s WHERE object_key = $1", s.table)
	var (
		content     []byte
		contentType sql.NullString
		size        int64
		etag        sql.NullString
		modified    time.Time
	)
	switch err := s.db.QueryRowContext(ctx, s.query(statement), key).Scan(&content, &contentType, &size, &etag, &modified); {
	case errors.Is(err, sql.ErrNoRows):
		return nil, spi.ObjectInfo{}, notFound("object", key)
	case err != nil:
		return nil, spi.ObjectInfo{}, err
	}
	info := spi.ObjectInfo{Key: key, Size: size, ContentType: contentType.String, ETag: etag.String, ModifiedAt: modified.UTC()}
	return io.NopCloser(bytes.NewReader(content)), info, nil
}

// Delete implements spi.ObjectStore.
func (s *sqlStorage) Delete(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, s.query(fmt.Sprintf("DELETE FROM %s WHERE object_key = $1", s.table)), key)
	return err
}

// List implements spi.ObjectStore.
func (s *sqlStorage) List(ctx context.Context, prefix string, limit int) ([]spi.ObjectInfo, error) {
	if limit <= 0 {
		limit = 1000
	}
	statement := fmt.Sprintf(`SELECT object_key, content_type, size_bytes, etag, modified_at FROM %s
		WHERE object_key LIKE $1 ESCAPE '\' ORDER BY object_key LIMIT $2`, s.table)
	rows, err := s.db.QueryContext(ctx, s.query(statement), escapeLike(prefix)+"%", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []spi.ObjectInfo
	for rows.Next() {
		var (
			info        spi.ObjectInfo
			contentType sql.NullString
			etag        sql.NullString
		)
		if err := rows.Scan(&info.Key, &contentType, &info.Size, &etag, &info.ModifiedAt); err != nil {
			return nil, err
		}
		info.ContentType, info.ETag = contentType.String, etag.String
		info.ModifiedAt = info.ModifiedAt.UTC()
		out = append(out, info)
	}
	return out, rows.Err()
}

var (
	_ spi.ObjectStore = (*fsStorage)(nil)
	_ spi.ObjectStore = (*sqlStorage)(nil)
)
