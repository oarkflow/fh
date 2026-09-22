package main

import (
	"context"
	"database/sql"
	"fmt"
)

// migrations is the application's schema. It runs on every start and every
// statement is written to be safe to re-run, which is what makes "the schema is
// part of the deployment" a workable claim rather than a hopeful one.
//
// The columns worth pointing at:
//
//   - inventory.reserved is a separate column from available, so a reservation is
//     a transfer between two numbers rather than a deletion. A CHECK keeps both
//     non-negative, which means an oversell is refused by the database even if the
//     application's arithmetic is wrong.
//   - outbox is an ordinary table. Rows are written inside the same transaction as
//     the domain rows, which is the only thing that makes delivery-at-least-once
//     honest; see effectstore.go.
//   - audit_log has no foreign key to users on purpose: an audit row must survive
//     the deletion of whatever it describes.
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS tenants (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		tier TEXT NOT NULL DEFAULT 'standard',
		region TEXT NOT NULL DEFAULT 'eu-central-1',
		suspended BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`INSERT INTO tenants (id, name, tier, region)
		VALUES ('acme', 'Acme Corp', 'enterprise', 'eu-central-1')
		ON CONFLICT (id) DO NOTHING`,
	`INSERT INTO tenants (id, name, tier, region, suspended)
		VALUES ('suspended-co', 'Suspended Co', 'standard', 'eu-central-1', TRUE)
		ON CONFLICT (id) DO NOTHING`,

	`CREATE TABLE IF NOT EXISTS users (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES tenants(id),
		email TEXT NOT NULL,
		name TEXT NOT NULL,
		password_hash TEXT NOT NULL,
		roles TEXT NOT NULL DEFAULT 'customer',
		disabled BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS users_tenant_email_idx ON users (tenant_id, email)`,

	`CREATE TABLE IF NOT EXISTS products (
		sku TEXT NOT NULL,
		tenant_id TEXT NOT NULL REFERENCES tenants(id),
		name TEXT NOT NULL,
		price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
		currency TEXT NOT NULL DEFAULT 'EUR',
		active BOOLEAN NOT NULL DEFAULT TRUE,
		PRIMARY KEY (tenant_id, sku)
	)`,
	`INSERT INTO products (tenant_id, sku, name, price_cents) VALUES
		('acme', 'WIDGET-100', 'Widget', 4999),
		('acme', 'LAPTOP-X', 'Laptop X', 129900),
		('acme', 'CABLE-1', 'USB-C cable', 1200)
		ON CONFLICT (tenant_id, sku) DO NOTHING`,

	`CREATE TABLE IF NOT EXISTS inventory (
		sku TEXT NOT NULL,
		tenant_id TEXT NOT NULL REFERENCES tenants(id),
		available INTEGER NOT NULL DEFAULT 0 CHECK (available >= 0),
		reserved INTEGER NOT NULL DEFAULT 0 CHECK (reserved >= 0),
		PRIMARY KEY (tenant_id, sku)
	)`,
	`INSERT INTO inventory (tenant_id, sku, available) VALUES
		('acme', 'WIDGET-100', 100),
		('acme', 'LAPTOP-X', 5),
		('acme', 'CABLE-1', 500)
		ON CONFLICT (tenant_id, sku) DO NOTHING`,

	`CREATE TABLE IF NOT EXISTS orders (
		id TEXT PRIMARY KEY,
		tenant_id TEXT NOT NULL REFERENCES tenants(id),
		customer_id TEXT NOT NULL REFERENCES users(id),
		sku TEXT NOT NULL,
		quantity INTEGER NOT NULL CHECK (quantity > 0),
		unit_price_cents BIGINT NOT NULL,
		total_cents BIGINT NOT NULL,
		currency TEXT NOT NULL DEFAULT 'EUR',
		status TEXT NOT NULL DEFAULT 'placed',
		idempotency_key TEXT,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (tenant_id, customer_id, created_at DESC)`,
	// A repeated request must find the order it already created, not create a
	// second one. The uniqueness is enforced here rather than by a read-then-write
	// in the application, because two concurrent requests would both pass the read.
	`CREATE UNIQUE INDEX IF NOT EXISTS orders_idempotency_idx
		ON orders (tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL`,

	`CREATE TABLE IF NOT EXISTS outbox (
		id BIGSERIAL PRIMARY KEY,
		execution_id TEXT NOT NULL,
		effect TEXT NOT NULL,
		payload JSONB NOT NULL,
		status TEXT NOT NULL DEFAULT 'pending',
		attempts INTEGER NOT NULL DEFAULT 0,
		last_error TEXT,
		visible_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		delivered_at TIMESTAMPTZ
	)`,
	`CREATE INDEX IF NOT EXISTS outbox_pending_idx ON outbox (status, visible_at)`,

	`CREATE TABLE IF NOT EXISTS effect_journal (
		tx_id TEXT PRIMARY KEY,
		execution_id TEXT NOT NULL,
		effects JSONB NOT NULL,
		state TEXT NOT NULL DEFAULT 'open',
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`CREATE INDEX IF NOT EXISTS effect_journal_state_idx ON effect_journal (state, created_at)`,

	`CREATE TABLE IF NOT EXISTS audit_log (
		id BIGSERIAL PRIMARY KEY,
		execution_id TEXT NOT NULL,
		intent TEXT NOT NULL,
		principal_id TEXT,
		tenant_id TEXT,
		policy TEXT,
		verdict TEXT,
		message TEXT,
		duration_ms DOUBLE PRECISION,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`,
	`CREATE INDEX IF NOT EXISTS audit_log_execution_idx ON audit_log (execution_id)`,
}

// migrate runs the schema. It stops at the first failure and names the statement,
// because a half-applied schema with a vague error is the worst thing to debug.
func migrate(ctx context.Context, db *sql.DB) error {
	for index, statement := range migrations {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("ref-app: migration %d failed: %w\n  statement: %s", index, err, statement)
		}
	}
	return nil
}
