package database

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/sqlite"
)

type User struct {
	ID        string `db:"id" json:"id"`
	Name      string `db:"name" json:"name"`
	Role      string `db:"role" json:"role"`
	CreatedAt string `db:"created_at" json:"createdAt"`
}

// Note is an owner-scoped resource used to demonstrate RBAC+ABAC together:
// any authenticated user may read/write their own notes (an ABAC "owner"
// policy keyed on UserID), while the admin role gets blanket access (an RBAC
// role permission) - see policy.authz.
type Note struct {
	ID        string `db:"id" json:"id"`
	UserID    string `db:"user_id" json:"userID"`
	Title     string `db:"title" json:"title"`
	Body      string `db:"body" json:"body"`
	CreatedAt string `db:"created_at" json:"createdAt"`
	UpdatedAt string `db:"updated_at" json:"updatedAt"`
}

// PoolConfig bounds how many concurrent connections the process opens and how
// long they live, so a slow query or a connection leak degrades gracefully
// under load instead of exhausting the database or the process's own file
// descriptors. Values are conservative defaults for a single-instance SQLite
// deployment; raise MaxOpenConns for a networked database (Postgres/MySQL)
// once the application scales beyond one process.
type PoolConfig = squealx.PoolConfig

func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
	}
}

// queryRetry is used for read-only, idempotent queries on the hot path
// (session/role lookups, note reads) so a single transient error (a locked
// SQLite file, a dropped connection) does not surface as a user-facing
// failure. Writes are never retried implicitly - see squealx.RetryPolicy.
var queryRetry = squealx.DefaultRetryPolicy()

type Store struct {
	DB    *squealx.DB
	Users squealx.Repository[User]
	Notes squealx.Repository[Note]
}

func Open(url string) (*Store, error) {
	return OpenWithPool(url, DefaultPoolConfig())
}

func OpenWithPool(url string, pool PoolConfig) (*Store, error) {
	if err := ensureSQLiteDirectory(url); err != nil {
		return nil, err
	}
	db, err := sqlite.Open(url, "app")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := db.ApplyPoolConfig(pool); err != nil {
		db.Close()
		return nil, fmt.Errorf("configure connection pool: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{
		DB:    db,
		Users: squealx.New[User](db, "users", "id"),
		Notes: squealx.New[Note](db, "notes", "id"),
	}, nil
}

func migrate(db *squealx.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			role TEXT NOT NULL DEFAULT 'viewer',
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS notes (
			id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			title TEXT NOT NULL,
			body TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
			updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
		)`,
		// Every note list/read is scoped to a single owner (see /api/notes),
		// so this index keeps that query O(log n) instead of a full table scan
		// as the table grows - the "no bottlenecks" requirement in practice.
		`CREATE INDEX IF NOT EXISTS idx_notes_user_id_created_at ON notes (user_id, created_at DESC)`,
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate database: %w", err)
		}
	}
	return nil
}

func ensureSQLiteDirectory(databaseURL string) error {
	if strings.HasPrefix(databaseURL, "file:") {
		path := strings.TrimPrefix(databaseURL, "file:")
		path = strings.SplitN(path, "?", 2)[0]
		if path == "" || path == ":memory:" || strings.HasPrefix(path, "/") {
			return nil
		}
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return fmt.Errorf("create database directory: %w", err)
		}
	}
	return nil
}

func (s *Store) Close() error { return s.DB.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.DB.PingContext(ctx) }

// EnsureUser upserts a user row and returns its role. It backs the single
// development login user today; swap it for a real signup/provisioning flow
// without changing any authorization code, since every downstream check
// (session roles, authz RBAC/ABAC) reads from this table, not from a
// hardcoded value.
func (s *Store) EnsureUser(ctx context.Context, id, name, role string) (User, error) {
	var existing User
	err := squealx.Retry(ctx, queryRetry, func(ctx context.Context) error {
		var findErr error
		existing, findErr = s.Users.First(ctx, map[string]any{"id": id})
		return findErr
	})
	if err == nil && existing.ID != "" {
		return existing, nil
	}
	user := User{ID: id, Name: name, Role: role}
	if err := s.Users.Create(ctx, &user); err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return user, nil
}

// RoleForUser resolves a user's current RBAC role from the database. Reads
// are retried (transient errors only, never a "not found") so a brief
// connection hiccup does not fail an authorization check outright.
func (s *Store) RoleForUser(ctx context.Context, id string) (string, error) {
	var user User
	err := squealx.Retry(ctx, queryRetry, func(ctx context.Context) error {
		var findErr error
		user, findErr = s.Users.First(ctx, map[string]any{"id": id})
		return findErr
	})
	if err != nil {
		return "", fmt.Errorf("resolve role for %q: %w", id, err)
	}
	return user.Role, nil
}
