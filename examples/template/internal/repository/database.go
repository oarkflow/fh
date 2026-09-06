package repository

import (
	"context"
	"fmt"

	"github.com/oarkflow/squealx"
	"github.com/oarkflow/squealx/drivers/sqlite"
)

// Database is the application's persistence health boundary. Domain repositories
// can be added beside this adapter without coupling the service layer to SQL.
type Database struct{ db *squealx.DB }

func OpenDatabase(enabled bool, driver, dsn, name string) (*Database, error) {
	if !enabled {
		return &Database{}, nil
	}
	if driver != "sqlite" {
		return nil, &UnsupportedDriverError{Driver: driver}
	}
	db, err := sqlite.Open(dsn, name)
	if err != nil {
		return nil, err
	}
	database := &Database{db: db}
	if err := database.Migrate(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return database, nil
}

func (d *Database) Migrate(ctx context.Context) error {
	if d == nil || d.db == nil {
		return nil
	}
	_, err := d.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS examples (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`)
	if err != nil {
		return fmt.Errorf("migrate examples: %w", err)
	}
	return nil
}

func (d *Database) List(ctx context.Context) ([]Item, error) {
	if d == nil || d.db == nil {
		return nil, nil
	}
	items := make([]Item, 0)
	if err := d.db.SelectContext(ctx, &items, `SELECT id, name FROM examples ORDER BY id`); err != nil {
		return nil, fmt.Errorf("list examples: %w", err)
	}
	return items, nil
}

func (d *Database) Add(ctx context.Context, name string) (Item, error) {
	if d == nil || d.db == nil {
		return Item{}, fmt.Errorf("database is disabled")
	}
	result, err := d.db.ExecContext(ctx, `INSERT INTO examples(name) VALUES (?)`, name)
	if err != nil {
		return Item{}, fmt.Errorf("insert example: %w", err)
	}
	id, err := result.LastInsertId()
	if err != nil {
		return Item{}, fmt.Errorf("read inserted id: %w", err)
	}
	return Item{ID: int(id), Name: name}, nil
}

func (d *Database) Health(ctx context.Context) error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.PingContext(ctx)
}
func (d *Database) Close() error {
	if d == nil || d.db == nil {
		return nil
	}
	return d.db.Close()
}

type UnsupportedDriverError struct{ Driver string }

func (e *UnsupportedDriverError) Error() string { return "unsupported database driver: " + e.Driver }
