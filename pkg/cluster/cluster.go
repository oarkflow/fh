package cluster

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// Node describes one fh process participating in a cluster. The package is
// dependency-free and intentionally backend-neutral; production deployments can
// implement Store with SQL, Redis, Consul, etc. MemoryStore is useful for tests
// and single-process coordination.
type Node struct {
	ID        string            `json:"id"`
	Address   string            `json:"address,omitempty"`
	Version   string            `json:"version,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	StartedAt time.Time         `json:"started_at"`
	LastSeen  time.Time         `json:"last_seen"`
	Draining  bool              `json:"draining"`
}

type Lease struct {
	Name      string    `json:"name"`
	Owner     string    `json:"owner"`
	ExpiresAt time.Time `json:"expires_at"`
	Token     string    `json:"token,omitempty"`
}

type Store interface {
	Heartbeat(ctx context.Context, node Node, ttl time.Duration) error
	Nodes(ctx context.Context, now time.Time) ([]Node, error)
	TryAcquire(ctx context.Context, name, owner string, ttl time.Duration) (Lease, bool, error)
	Renew(ctx context.Context, name, owner string, ttl time.Duration) (Lease, bool, error)
	Release(ctx context.Context, name, owner string) error
}

type Coordinator struct {
	store Store
	node  Node
	ttl   time.Duration
	now   func() time.Time
}

type Config struct {
	Store Store
	Node  Node
	TTL   time.Duration
	Now   func() time.Time
}

func New(cfg Config) (*Coordinator, error) {
	if cfg.Store == nil {
		return nil, errors.New("fh/cluster: store required")
	}
	if cfg.Node.ID == "" {
		return nil, errors.New("fh/cluster: node id required")
	}
	if cfg.TTL <= 0 {
		cfg.TTL = 15 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Node.StartedAt.IsZero() {
		cfg.Node.StartedAt = cfg.Now().UTC()
	}
	return &Coordinator{store: cfg.Store, node: cfg.Node, ttl: cfg.TTL, now: cfg.Now}, nil
}

func (c *Coordinator) Node() Node { return c.node }
func (c *Coordinator) Heartbeat(ctx context.Context) error {
	if c == nil {
		return errors.New("fh/cluster: nil coordinator")
	}
	n := c.node
	n.LastSeen = c.now().UTC()
	return c.store.Heartbeat(ctx, n, c.ttl)
}
func (c *Coordinator) Nodes(ctx context.Context) ([]Node, error) {
	return c.store.Nodes(ctx, c.now().UTC())
}
func (c *Coordinator) TryLead(ctx context.Context, name string) (Lease, bool, error) {
	return c.store.TryAcquire(ctx, name, c.node.ID, c.ttl)
}
func (c *Coordinator) RenewLeadership(ctx context.Context, name string) (Lease, bool, error) {
	return c.store.Renew(ctx, name, c.node.ID, c.ttl)
}
func (c *Coordinator) ReleaseLeadership(ctx context.Context, name string) error {
	return c.store.Release(ctx, name, c.node.ID)
}

// MemoryStore is safe for tests and single-process demos. It is not a
// distributed backend; use the Store interface for production persistence.
type MemoryStore struct {
	mu      sync.Mutex
	nodes   map[string]Node
	nodeTTL map[string]time.Time
	leases  map[string]Lease
	now     func() time.Time
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{nodes: map[string]Node{}, nodeTTL: map[string]time.Time{}, leases: map[string]Lease{}, now: time.Now}
}
func (s *MemoryStore) Heartbeat(ctx context.Context, node Node, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if node.LastSeen.IsZero() {
		node.LastSeen = s.now().UTC()
	}
	s.nodes[node.ID] = cloneNode(node)
	s.nodeTTL[node.ID] = s.now().Add(ttl)
	return nil
}
func (s *MemoryStore) Nodes(ctx context.Context, now time.Time) ([]Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = s.now()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Node, 0, len(s.nodes))
	for id, n := range s.nodes {
		if exp := s.nodeTTL[id]; exp.IsZero() || exp.After(now) {
			out = append(out, cloneNode(n))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}
func (s *MemoryStore) TryAcquire(ctx context.Context, name, owner string, ttl time.Duration) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	if l, ok := s.leases[name]; ok && l.ExpiresAt.After(now) && l.Owner != "" && l.Owner != owner {
		return l, false, nil
	}
	l := Lease{Name: name, Owner: owner, ExpiresAt: now.Add(ttl), Token: owner + ":" + name}
	s.leases[name] = l
	return l, true, nil
}
func (s *MemoryStore) Renew(ctx context.Context, name, owner string, ttl time.Duration) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	l := s.leases[name]
	if l.Owner != "" && l.Owner != owner && l.ExpiresAt.After(now) {
		return l, false, nil
	}
	l = Lease{Name: name, Owner: owner, ExpiresAt: now.Add(ttl), Token: owner + ":" + name}
	s.leases[name] = l
	return l, true, nil
}
func (s *MemoryStore) Release(ctx context.Context, name, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if l := s.leases[name]; l.Owner == owner {
		delete(s.leases, name)
	}
	return nil
}
func cloneNode(n Node) Node {
	if len(n.Metadata) > 0 {
		m := map[string]string{}
		for k, v := range n.Metadata {
			m[k] = v
		}
		n.Metadata = m
	}
	return n
}
