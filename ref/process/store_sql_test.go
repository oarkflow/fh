package process

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

// TestSQLiteStoreConformance runs the full store conformance suite against SQLite.
// This validates that the PostgreSQL-shaped SQL, placeholder rebinding, and
// dialect-specific upsert/lock logic all work correctly for the sqlite dialect.
func TestSQLiteStoreConformance(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SQL conformance in short mode")
	}

	runStoreConformance(t, func(t *testing.T) Store {
		t.Helper()
		db, err := sql.Open("sqlite", ":memory:")
		if err != nil {
			t.Fatalf("sqlite open: %v", err)
		}
		t.Cleanup(func() { db.Close() })

		// SQLite serializes writes, so one connection is safest for tests.
		db.SetMaxOpenConns(1)

		store, err := NewSQLStore(SQLStoreConfig{
			DB:          db,
			Dialect:     "sqlite",
			TablePrefix: "test",
		})
		if err != nil {
			t.Fatalf("new sql store: %v", err)
		}
		return store
	})
}

// TestSQLiteMigrate verifies that the full schema creates without error.
func TestSQLiteMigrate(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite open: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	store, err := NewSQLStore(SQLStoreConfig{
		DB:          db,
		Dialect:     "sqlite",
		TablePrefix: "migrate_test",
	})
	if err != nil {
		t.Fatalf("new sql store: %v", err)
	}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Verify key tables exist
	tables := []string{
		"migrate_test_runs", "migrate_test_steps", "migrate_test_timers",
		"migrate_test_subscriptions", "migrate_test_joins", "migrate_test_leases",
		"migrate_test_tasks", "migrate_test_events", "migrate_test_sequences",
	}
	for _, table := range tables {
		var name string
		err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
		if err != nil {
			t.Errorf("table %s not found after migration: %v", table, err)
		}
	}
}
