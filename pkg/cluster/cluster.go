package cluster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/oarkflow/fh/pkg/storage/kv"
)

// Node describes one fh process participating in a cluster. The package is
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

const stateKey = "cluster:state"

type Coordinator struct {
	store kv.Store
	node  Node
	ttl   time.Duration
	now   func() time.Time
}

type Config struct {
	Store kv.Store
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
	return Heartbeat(ctx, c.store, n, c.ttl, c.now)
}
func (c *Coordinator) Nodes(ctx context.Context) ([]Node, error) {
	return Nodes(ctx, c.store, c.now().UTC())
}
func (c *Coordinator) TryLead(ctx context.Context, name string) (Lease, bool, error) {
	return TryAcquire(ctx, c.store, name, c.node.ID, c.ttl, c.now)
}
func (c *Coordinator) RenewLeadership(ctx context.Context, name string) (Lease, bool, error) {
	return Renew(ctx, c.store, name, c.node.ID, c.ttl, c.now)
}
func (c *Coordinator) ReleaseLeadership(ctx context.Context, name string) error {
	return Release(ctx, c.store, name, c.node.ID)
}

type state struct {
	Nodes  map[string]nodeEntry `json:"nodes"`
	Leases map[string]Lease     `json:"leases"`
}

type nodeEntry struct {
	Node      Node      `json:"node"`
	ExpiresAt time.Time `json:"expires_at"`
}

func Heartbeat(ctx context.Context, store kv.Store, node Node, ttl time.Duration, nowFunc func() time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	now := nowOrDefault(nowFunc)
	if node.LastSeen.IsZero() {
		node.LastSeen = now.UTC()
	}
	return mutateState(store, func(st *state) error {
		st.Nodes[node.ID] = nodeEntry{Node: cloneNode(node), ExpiresAt: now.Add(ttl)}
		return nil
	})
}

func Nodes(ctx context.Context, store kv.Store, now time.Time) ([]Node, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if now.IsZero() {
		now = time.Now()
	}
	st, err := loadState(store)
	if err != nil {
		return nil, err
	}
	out := make([]Node, 0, len(st.Nodes))
	for _, entry := range st.Nodes {
		if entry.ExpiresAt.IsZero() || entry.ExpiresAt.After(now) {
			out = append(out, cloneNode(entry.Node))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func TryAcquire(ctx context.Context, store kv.Store, name, owner string, ttl time.Duration, nowFunc func() time.Time) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	now := nowOrDefault(nowFunc)
	var lease Lease
	var ok bool
	err := mutateState(store, func(st *state) error {
		if l, exists := st.Leases[name]; exists && l.ExpiresAt.After(now) && l.Owner != "" && l.Owner != owner {
			lease = l
			ok = false
			return nil
		}
		lease = Lease{Name: name, Owner: owner, ExpiresAt: now.Add(ttl), Token: generateToken()}
		st.Leases[name] = lease
		ok = true
		return nil
	})
	return lease, ok, err
}

func Renew(ctx context.Context, store kv.Store, name, owner string, ttl time.Duration, nowFunc func() time.Time) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	if ttl <= 0 {
		ttl = 15 * time.Second
	}
	now := nowOrDefault(nowFunc)
	var lease Lease
	var ok bool
	err := mutateState(store, func(st *state) error {
		l := st.Leases[name]
		if l.Owner != "" && l.Owner != owner && l.ExpiresAt.After(now) {
			lease = l
			ok = false
			return nil
		}
		lease = Lease{Name: name, Owner: owner, ExpiresAt: now.Add(ttl), Token: generateToken()}
		st.Leases[name] = lease
		ok = true
		return nil
	})
	return lease, ok, err
}

func Release(ctx context.Context, store kv.Store, name, owner string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return mutateState(store, func(st *state) error {
		if l := st.Leases[name]; l.Owner == owner {
			delete(st.Leases, name)
		}
		return nil
	})
}

func loadState(store kv.Store) (state, error) {
	st := emptyState()
	data, ok, err := store.Get(stateKey)
	if err != nil || !ok {
		return st, err
	}
	if err := json.Unmarshal(data, &st); err != nil {
		return st, err
	}
	if st.Nodes == nil {
		st.Nodes = map[string]nodeEntry{}
	}
	if st.Leases == nil {
		st.Leases = map[string]Lease{}
	}
	return st, nil
}

func mutateState(store kv.Store, fn func(*state) error) error {
	return store.Mutate(stateKey, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		st := emptyState()
		if exists {
			if err := json.Unmarshal(current, &st); err != nil {
				return nil, 0, false, err
			}
			if st.Nodes == nil {
				st.Nodes = map[string]nodeEntry{}
			}
			if st.Leases == nil {
				st.Leases = map[string]Lease{}
			}
		}
		if err := fn(&st); err != nil {
			return nil, 0, false, err
		}
		data, err := json.Marshal(st)
		if err != nil {
			return nil, 0, false, err
		}
		return data, 0, true, nil
	})
}

func emptyState() state {
	return state{Nodes: map[string]nodeEntry{}, Leases: map[string]Lease{}}
}

func nowOrDefault(fn func() time.Time) time.Time {
	if fn == nil {
		return time.Now()
	}
	return fn()
}

func generateToken() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
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
