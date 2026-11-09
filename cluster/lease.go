// Package cluster provides explicit fenced ownership for deployments that
// route MIRA sessions across multiple nodes. The in-memory implementation is
// for tests only; production processes must share a transactional lease store.
package cluster

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"time"
)

var ErrHeld = errors.New("cluster: identity owned by another live node")
var ErrFenced = errors.New("cluster: lease lost, expired, or superseded")
var ErrInvalid = errors.New("cluster: invalid lease request")

type Lease struct {
	Identity, Owner string
	Fence           uint64
	Expires         time.Time
}
type LeaseStore interface {
	Acquire(context.Context, string, string, time.Duration) (Lease, error)
	Renew(context.Context, Lease, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	Current(context.Context, string) (Lease, error)
}

// PreferredOwner uses rendezvous hashing so membership changes redistribute
// comparatively few identities. It is only a routing hint: LeaseStore fencing
// decides the authoritative owner.
func PreferredOwner(identity string, nodes []string) string {
	best := ""
	var score [32]byte
	for _, node := range nodes {
		if node == "" {
			continue
		}
		sum := sha256.Sum256([]byte(identity + "\x00" + node))
		if best == "" || greater(sum, score) {
			best = node
			score = sum
		}
	}
	return best
}
func greater(a, b [32]byte) bool {
	for i := range a {
		if a[i] > b[i] {
			return true
		}
		if a[i] < b[i] {
			return false
		}
	}
	return false
}
func Membership(nodes []string) []string {
	m := make(map[string]struct{})
	for _, n := range nodes {
		if n != "" {
			m[n] = struct{}{}
		}
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

type MemoryStore struct {
	mu       sync.Mutex
	leases   map[string]Lease
	counters map[string]uint64
	now      func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{leases: make(map[string]Lease), counters: make(map[string]uint64), now: time.Now}
}
func valid(identity, owner string, ttl time.Duration) bool {
	return len(identity) > 0 && len(identity) <= 256 && len(owner) > 0 && len(owner) <= 256 && ttl >= time.Second && ttl <= time.Hour
}
func (s *MemoryStore) Acquire(ctx context.Context, identity, owner string, ttl time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if !valid(identity, owner, ttl) {
		return Lease{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if old, ok := s.leases[identity]; ok && old.Expires.After(now) && old.Owner != owner {
		return Lease{}, ErrHeld
	}
	s.counters[identity]++
	v := Lease{Identity: identity, Owner: owner, Fence: s.counters[identity], Expires: now.Add(ttl)}
	s.leases[identity] = v
	return v, nil
}
func (s *MemoryStore) Renew(ctx context.Context, v Lease, ttl time.Duration) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	if !valid(v.Identity, v.Owner, ttl) {
		return Lease{}, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.leases[v.Identity]
	if !ok || old.Owner != v.Owner || old.Fence != v.Fence || !old.Expires.After(s.now()) {
		return Lease{}, ErrFenced
	}
	old.Expires = s.now().Add(ttl)
	s.leases[v.Identity] = old
	return old, nil
}
func (s *MemoryStore) Release(ctx context.Context, v Lease) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, ok := s.leases[v.Identity]
	if !ok || old.Owner != v.Owner || old.Fence != v.Fence {
		return ErrFenced
	}
	delete(s.leases, v.Identity)
	return nil
}
func (s *MemoryStore) Current(ctx context.Context, identity string) (Lease, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.leases[identity]
	if !ok || !v.Expires.After(s.now()) {
		return Lease{}, ErrFenced
	}
	return v, nil
}

// PostgreSQLStore expects the DDL in sql/schema.sql and a caller-supplied
// database/sql driver. DB-time-based conditional UPSERT and fencing prevent
// multiple owners from simultaneously committing one identity's outbox.
type PostgreSQLStore struct{ DB *sql.DB }

func (s PostgreSQLStore) Acquire(ctx context.Context, identity, owner string, ttl time.Duration) (Lease, error) {
	if s.DB == nil || !valid(identity, owner, ttl) {
		return Lease{}, ErrInvalid
	}
	var v Lease
	v.Identity = identity
	// Always increment the fence, including reacquisition by the same node.
	err := s.DB.QueryRowContext(ctx, `INSERT INTO mira_leases(identity,owner,fence,expires_at)
 VALUES($1,$2,1,now() + ($3 * interval '1 microsecond'))
 ON CONFLICT (identity) DO UPDATE SET
 owner=EXCLUDED.owner, fence=mira_leases.fence+1,
 expires_at=EXCLUDED.expires_at
 WHERE mira_leases.expires_at <= now() OR mira_leases.owner=EXCLUDED.owner
 RETURNING owner,fence,expires_at`, identity, owner, ttl.Microseconds()).Scan(&v.Owner, &v.Fence, &v.Expires)
	if errors.Is(err, sql.ErrNoRows) {
		return Lease{}, ErrHeld
	}
	if err != nil {
		return Lease{}, err
	}
	return v, nil
}
func (s PostgreSQLStore) Renew(ctx context.Context, v Lease, ttl time.Duration) (Lease, error) {
	if s.DB == nil || !valid(v.Identity, v.Owner, ttl) {
		return Lease{}, ErrInvalid
	}
	var expiry time.Time
	err := s.DB.QueryRowContext(ctx, `UPDATE mira_leases SET expires_at=now()+($4 * interval '1 microsecond')
 WHERE identity=$1 AND owner=$2 AND fence=$3 AND expires_at>now()
 RETURNING expires_at`, v.Identity, v.Owner, v.Fence, ttl.Microseconds()).Scan(&expiry)
	if errors.Is(err, sql.ErrNoRows) {
		return Lease{}, ErrFenced
	}
	if err != nil {
		return Lease{}, err
	}
	v.Expires = expiry
	return v, nil
}
func (s PostgreSQLStore) Release(ctx context.Context, v Lease) error {
	if s.DB == nil {
		return ErrInvalid
	}
	res, err := s.DB.ExecContext(ctx, `UPDATE mira_leases SET expires_at='epoch'::timestamptz
 WHERE identity=$1 AND owner=$2 AND fence=$3`, v.Identity, v.Owner, v.Fence)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrFenced
	}
	return nil
}
func (s PostgreSQLStore) Current(ctx context.Context, id string) (Lease, error) {
	if s.DB == nil {
		return Lease{}, ErrInvalid
	}
	var v Lease
	v.Identity = id
	err := s.DB.QueryRowContext(ctx, `SELECT owner,fence,expires_at FROM mira_leases
 WHERE identity=$1 AND expires_at>now()`, id).Scan(&v.Owner, &v.Fence, &v.Expires)
	if errors.Is(err, sql.ErrNoRows) {
		return Lease{}, ErrFenced
	}
	return v, err
}

// MonotonicKey encodes the fence as an opaque 8-byte value suitable for
// inclusion in a durable event's authenticated ownership metadata.
func MonotonicKey(l Lease) [8]byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], l.Fence)
	return b
}
