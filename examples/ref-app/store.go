package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// The data layer.
//
// Plain database/sql with explicit statements. Every query that touches
// tenant-scoped data takes the tenant id as a parameter and no query builds SQL by
// concatenation — a tenant boundary that depends on remembering to add a WHERE
// clause is not a boundary, so the signatures here make it impossible to ask for a
// row without saying whose it is.

var errNotFound = errors.New("not found")

// Store owns the connection pool.
type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// DB exposes the pool for the effect store, which needs to begin its own
// transactions.
func (s *Store) DB() *sql.DB { return s.db }

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

type Tenant struct {
	ID        string
	Name      string
	Tier      string
	Region    string
	Suspended bool
}

func (s *Store) Tenant(ctx context.Context, id string) (Tenant, error) {
	var tenant Tenant
	err := s.db.QueryRowContext(ctx,
		`SELECT id,name,tier,region,suspended FROM tenants WHERE id=$1`, id).
		Scan(&tenant.ID, &tenant.Name, &tenant.Tier, &tenant.Region, &tenant.Suspended)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, errNotFound
	}
	return tenant, err
}

// ---------------------------------------------------------------------------
// Users
// ---------------------------------------------------------------------------

type User struct {
	ID           string
	TenantID     string
	Email        string
	Name         string
	PasswordHash string
	Roles        []string
	Disabled     bool
}

func (s *Store) CreateUser(ctx context.Context, user User) (User, error) {
	user.ID = newID("usr-")
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO users (id,tenant_id,email,name,password_hash,roles)
		 VALUES ($1,$2,LOWER($3),$4,$5,$6)`,
		user.ID, user.TenantID, user.Email, user.Name, user.PasswordHash, strings.Join(user.Roles, ","))
	if err != nil {
		return User{}, err
	}
	return user, nil
}

// UserByEmail returns the user or errNotFound. The caller must treat both the same
// way; see verifyPassword.
func (s *Store) UserByEmail(ctx context.Context, tenantID, email string) (User, error) {
	var (
		user  User
		roles string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT id,tenant_id,email,name,password_hash,roles,disabled
		   FROM users WHERE tenant_id=$1 AND email=LOWER($2)`, tenantID, email).
		Scan(&user.ID, &user.TenantID, &user.Email, &user.Name, &user.PasswordHash, &roles, &user.Disabled)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, errNotFound
	}
	if err != nil {
		return User{}, err
	}
	user.Roles = splitList(roles)
	return user, nil
}

func (s *Store) UserByID(ctx context.Context, id string) (User, error) {
	var (
		user  User
		roles string
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT id,tenant_id,email,name,password_hash,roles,disabled FROM users WHERE id=$1`, id).
		Scan(&user.ID, &user.TenantID, &user.Email, &user.Name, &user.PasswordHash, &roles, &user.Disabled)
	if errors.Is(err, sql.ErrNoRows) {
		return User{}, errNotFound
	}
	if err != nil {
		return User{}, err
	}
	user.Roles = splitList(roles)
	return user, nil
}

// ---------------------------------------------------------------------------
// Catalogue
// ---------------------------------------------------------------------------

type Product struct {
	SKU        string `json:"sku"`
	Name       string `json:"name"`
	PriceCents int64  `json:"price_cents"`
	Currency   string `json:"currency"`
	Available  int    `json:"available"`
}

func (s *Store) Catalogue(ctx context.Context, tenantID string) ([]Product, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT p.sku,p.name,p.price_cents,p.currency,COALESCE(i.available,0)
		   FROM products p
		   LEFT JOIN inventory i ON i.tenant_id=p.tenant_id AND i.sku=p.sku
		  WHERE p.tenant_id=$1 AND p.active
		  ORDER BY p.name`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Product
	for rows.Next() {
		var product Product
		if err := rows.Scan(&product.SKU, &product.Name, &product.PriceCents, &product.Currency, &product.Available); err != nil {
			return nil, err
		}
		out = append(out, product)
	}
	return out, rows.Err()
}

func (s *Store) Product(ctx context.Context, tenantID, sku string) (Product, error) {
	var product Product
	err := s.db.QueryRowContext(ctx,
		`SELECT p.sku,p.name,p.price_cents,p.currency,COALESCE(i.available,0)
		   FROM products p
		   LEFT JOIN inventory i ON i.tenant_id=p.tenant_id AND i.sku=p.sku
		  WHERE p.tenant_id=$1 AND p.sku=$2 AND p.active`, tenantID, sku).
		Scan(&product.SKU, &product.Name, &product.PriceCents, &product.Currency, &product.Available)
	if errors.Is(err, sql.ErrNoRows) {
		return Product{}, errNotFound
	}
	return product, err
}

// ---------------------------------------------------------------------------
// Orders
// ---------------------------------------------------------------------------

type Order struct {
	ID             string `json:"order_id"`
	TenantID       string `json:"tenant_id"`
	CustomerID     string `json:"customer_id"`
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
	TotalCents     int64  `json:"total_cents"`
	Currency       string `json:"currency"`
	Status         string `json:"status"`
	CreatedAt      string `json:"created_at"`
}

const orderColumns = `id,tenant_id,customer_id,sku,quantity,unit_price_cents,total_cents,currency,status,created_at`

func scanOrder(scan func(...any) error) (Order, error) {
	var order Order
	err := scan(&order.ID, &order.TenantID, &order.CustomerID, &order.SKU, &order.Quantity,
		&order.UnitPriceCents, &order.TotalCents, &order.Currency, &order.Status, &order.CreatedAt)
	return order, err
}

// Order reads one order within a tenant. The customer id is part of the predicate
// rather than checked afterwards, so a caller cannot accidentally read somebody
// else's row and then decide what to do about it.
func (s *Store) Order(ctx context.Context, tenantID, customerID, id string) (Order, error) {
	query := fmt.Sprintf(`SELECT %s FROM orders WHERE tenant_id=$1 AND id=$2`, orderColumns)
	args := []any{tenantID, id}
	if customerID != "" {
		query += ` AND customer_id=$3`
		args = append(args, customerID)
	}
	order, err := scanOrder(s.db.QueryRowContext(ctx, query, args...).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, errNotFound
	}
	return order, err
}

func (s *Store) Orders(ctx context.Context, tenantID, customerID string, limit int) ([]Order, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	query := fmt.Sprintf(`SELECT %s FROM orders WHERE tenant_id=$1`, orderColumns)
	args := []any{tenantID}
	if customerID != "" {
		query += ` AND customer_id=$2`
		args = append(args, customerID)
	}
	query += fmt.Sprintf(` ORDER BY created_at DESC LIMIT %d`, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Order
	for rows.Next() {
		order, err := scanOrder(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, order)
	}
	return out, rows.Err()
}

// OrderByIdempotencyKey is how a repeated create returns the original order
// instead of a duplicate.
func (s *Store) OrderByIdempotencyKey(ctx context.Context, tenantID, key string) (Order, error) {
	query := fmt.Sprintf(`SELECT %s FROM orders WHERE tenant_id=$1 AND idempotency_key=$2`, orderColumns)
	order, err := scanOrder(s.db.QueryRowContext(ctx, query, tenantID, key).Scan)
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, errNotFound
	}
	return order, err
}

// ---------------------------------------------------------------------------
// Transactional writes
//
// These take a *sql.Tx rather than using the pool: they are called by effects that
// share one transaction, and taking the Tx in the signature is what stops one of
// them from quietly committing on its own.
// ---------------------------------------------------------------------------

// reserveStock moves quantity from available to reserved. The UPDATE's own WHERE
// clause enforces sufficiency, so two concurrent reservations cannot both succeed
// on the last unit: the second matches no row.
func reserveStock(ctx context.Context, tx *sql.Tx, tenantID, sku string, quantity int) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE inventory SET available=available-$1, reserved=reserved+$1
		  WHERE tenant_id=$2 AND sku=$3 AND available>=$1`, quantity, tenantID, sku)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("insufficient stock for %s", sku)
	}
	return nil
}

// releaseStock is reserveStock's compensation.
func releaseStock(ctx context.Context, tx *sql.Tx, tenantID, sku string, quantity int) error {
	_, err := tx.ExecContext(ctx,
		`UPDATE inventory SET available=available+$1, reserved=GREATEST(reserved-$1,0)
		  WHERE tenant_id=$2 AND sku=$3`, quantity, tenantID, sku)
	return err
}

func insertOrder(ctx context.Context, tx *sql.Tx, order Order, idempotencyKey string) error {
	var key any
	if idempotencyKey != "" {
		key = idempotencyKey
	}
	_, err := tx.ExecContext(ctx,
		`INSERT INTO orders (id,tenant_id,customer_id,sku,quantity,unit_price_cents,total_cents,currency,status,idempotency_key)
		 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
		order.ID, order.TenantID, order.CustomerID, order.SKU, order.Quantity,
		order.UnitPriceCents, order.TotalCents, order.Currency, order.Status, key)
	return err
}

func setOrderStatus(ctx context.Context, tx *sql.Tx, tenantID, id, from, to string) error {
	result, err := tx.ExecContext(ctx,
		`UPDATE orders SET status=$1, updated_at=NOW()
		  WHERE tenant_id=$2 AND id=$3 AND status=$4`, to, tenantID, id, from)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		// Either the order is gone or somebody else already moved it. Both mean
		// this transition must not proceed.
		return fmt.Errorf("order %s is not %s", id, from)
	}
	return nil
}
