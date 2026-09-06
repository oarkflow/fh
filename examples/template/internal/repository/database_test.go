package repository

import (
	"context"
	"path/filepath"
	"testing"
)

func TestDatabaseCRUD(t *testing.T) {
	dsn := "file:" + filepath.Join(t.TempDir(), "app.db") + "?_pragma=foreign_keys(1)"
	db, err := OpenDatabase(true, "sqlite", dsn, "test")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	created, err := db.Add(context.Background(), "persisted")
	if err != nil {
		t.Fatal(err)
	}
	items, err := db.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if created.ID < 1 || len(items) != 1 || items[0].Name != "persisted" {
		t.Fatalf("created=%+v items=%+v", created, items)
	}
}
