package process

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

// TestMySQLStoreConformance runs the full store conformance suite against MySQL.
//
// Set the TEST_MYSQL_DSN environment variable to a valid MySQL DSN to enable:
//
//	TEST_MYSQL_DSN="user:pass@tcp(127.0.0.1:3306)/testdb?parseTime=true" go test ./process/ -run TestMySQL -v
//
// Without the variable, the test is skipped.
func TestMySQLStoreConformance(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; skipping MySQL conformance")
	}

	runStoreConformance(t, func(t *testing.T) Store {
		t.Helper()
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			t.Fatalf("mysql open: %v", err)
		}
		t.Cleanup(func() { db.Close() })

		if err := db.Ping(); err != nil {
			t.Skipf("mysql not reachable: %v", err)
		}

		store, err := NewSQLStore(SQLStoreConfig{
			DB:          db,
			Dialect:     "mysql",
			TablePrefix: "test_mysql",
		})
		if err != nil {
			t.Fatalf("new sql store: %v", err)
		}
		return store
	})
}

// TestMySQLMigrate verifies the full schema creates without error on MySQL.
func TestMySQLMigrate(t *testing.T) {
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; skipping MySQL migration test")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatalf("mysql open: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skipf("mysql not reachable: %v", err)
	}

	store, err := NewSQLStore(SQLStoreConfig{
		DB:          db,
		Dialect:     "mysql",
		TablePrefix: "migrate_mysql",
	})
	if err != nil {
		t.Fatalf("new sql store: %v", err)
	}
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Verify key tables exist
	tables := []string{
		"migrate_mysql_runs", "migrate_mysql_steps", "migrate_mysql_timers",
		"migrate_mysql_subscriptions", "migrate_mysql_joins", "migrate_mysql_leases",
		"migrate_mysql_tasks", "migrate_mysql_events", "migrate_mysql_sequences",
	}
	for _, table := range tables {
		var name string
		err := db.QueryRow("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?", table).Scan(&name)
		if err != nil {
			t.Errorf("table %s not found after migration: %v", table, err)
		}
	}
}
