package ipthrottle

import (
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/storage/kv"
)

type RejectHandler func(fh.Ctx, string, int) error

// ErrCapacityExceeded is returned by a Store's Increment when the store is
// already tracking cfg.MaxIPs distinct keys and the incoming key is new.
var ErrCapacityExceeded = errors.New("ipthrottle: capacity exceeded")

type Config struct {
	MaxPerIP  int
	GlobalMax int
	MaxIPs    int
	Window    time.Duration
	KeyFunc   func(fh.Ctx) string
	Reject    RejectHandler

	// Store holds per-IP window/count state, keyed by IP and JSON-encoded
	// via Increment. Defaults to a new kv.MemoryStore, which reproduces the
	// exact map+mutex+sweep behavior this package has always used. Pass a
	// kv.FileStore (see github.com/oarkflow/fh/pkg/storage/kv) to persist
	// counters across restarts.
	Store kv.Store
}

func New(cfg Config) fh.HandlerFunc {
	cfg = normalize(cfg)
	var active atomic.Int64

	return func(c fh.Ctx) error {
		key := cfg.KeyFunc(c)

		gConns := active.Add(1)
		defer active.Add(-1)

		if cfg.GlobalMax > 0 && int(gConns) > cfg.GlobalMax {
			return cfg.Reject(c, key, int(gConns))
		}

		ip := extractIP(key)
		now := time.Now()

		count, err := Increment(cfg.Store, ip, cfg.Window, cfg.MaxIPs, now)
		if err != nil {
			if errors.Is(err, ErrCapacityExceeded) {
				return cfg.Reject(c, key, 0)
			}
			return err
		}

		if cfg.MaxPerIP > 0 && count > cfg.MaxPerIP {
			return cfg.Reject(c, key, count)
		}

		return c.Next()
	}
}

func normalize(cfg Config) Config {
	if cfg.MaxPerIP <= 0 {
		cfg.MaxPerIP = 100
	}
	if cfg.GlobalMax <= 0 {
		cfg.GlobalMax = 10000
	}
	if cfg.MaxIPs <= 0 {
		cfg.MaxIPs = 100000
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Minute
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = func(c fh.Ctx) string { return c.IP() }
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, _ string, _ int) error {
			seconds := max(1, int((cfg.Window+time.Second-1)/time.Second))
			c.Set("Retry-After", strconv.Itoa(seconds))
			return c.Status(fh.StatusTooManyRequests).SendString("Too Many Requests")
		}
	}
	if cfg.Store == nil {
		cfg.Store = kv.NewMemoryStore()
	}
	return cfg
}

func extractIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

// bucketState is the JSON-encoded value held per key inside the underlying
// kv.Store: the current fixed window's count and the time it started.
type bucketState struct {
	Count       int       `json:"count"`
	WindowStart time.Time `json:"window_start"`
}

// Increment records one request for key within window (relative to now)
// against store, resetting the count if the previous window has elapsed,
// and returns the count so far in the current window. Per-key state (count,
// window start) is JSON-encoded and mutated race-free via kv.Store.Mutate,
// which reproduces the exact "reset on elapsed window, otherwise increment"
// semantics this package has always had, on top of any kv.Store
// implementation (kv.MemoryStore, kv.FileStore, or otherwise).
//
// If store is not yet tracking key and is already at maxKeys distinct keys
// (maxKeys <= 0 means unlimited), Increment returns ErrCapacityExceeded
// without recording the request.
//
// The maxKeys cardinality guard is checked via a Get+Len pair before the
// Mutate call, since only Mutate holds the per-key lock and kv.Store has no
// facility for locking a key that does not exist yet while a separate,
// store-wide Len() check runs. This means the check is a soft/approximate
// cap under heavy concurrent creation of brand-new distinct keys: two
// concurrent Increment calls for two different new keys can both observe
// Len() < maxKeys and both be admitted, so the cardinality bound can be
// exceeded by a small margin under contention. The pre-retrofit
// implementation held a single mutex across the entire check-then-insert
// sequence and so enforced an exact bound; kv.Store's per-key Mutate lock
// does not extend to a store-wide cardinality decision, so this is a
// deliberate, documented trade-off rather than an oversight.
func Increment(store kv.Store, key string, window time.Duration, maxKeys int, now time.Time) (int, error) {
	if maxKeys > 0 {
		if _, exists, err := store.Get(key); err != nil {
			return 0, err
		} else if !exists {
			n, err := store.Len()
			if err != nil {
				return 0, err
			}
			if n >= maxKeys {
				return 0, ErrCapacityExceeded
			}
		}
	}

	var count int
	err := store.Mutate(key, func(current []byte, exists bool) ([]byte, time.Duration, bool, error) {
		var b bucketState
		if exists {
			if err := json.Unmarshal(current, &b); err != nil {
				exists = false
			}
		}
		if !exists || now.Sub(b.WindowStart) >= window {
			b.Count = 0
			b.WindowStart = now
		}
		b.Count++
		count = b.Count
		next, err := json.Marshal(b)
		if err != nil {
			return nil, 0, false, err
		}
		return next, window, true, nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
