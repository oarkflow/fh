package source

import "sync"

// Coalescer deduplicates identical in-flight source requests.
//
// When multiple goroutines simultaneously request the same key, only the first
// one executes the fetch function. All other callers wait for and receive the
// same result. Once the call completes, the key is released — subsequent
// requests trigger a new fetch.
//
// Unlike caching, coalescing only deduplicates concurrent in-flight requests.
// It doesn't store results beyond the lifetime of the call. Use Cache for TTL-based
// result reuse; use Coalescer when 1000 simultaneous requests asking for the same
// thing should only hit the source once.
type Coalescer struct {
	mu    sync.Mutex
	calls map[string]*coalesceCall
}

type coalesceCall struct {
	wg  sync.WaitGroup
	val any
	err error
}

// NewCoalescer creates a new Coalescer.
func NewCoalescer() *Coalescer {
	return &Coalescer{
		calls: make(map[string]*coalesceCall),
	}
}

// Do executes fn only if no identical request (same key) is already in flight.
// If one is, it blocks until that call completes and returns the same result.
//
// The key should encode the operation and parameters (e.g. "users:id=123" or
// a hash). Do not include authorization scope in the key unless different scopes
// should share results — usually they should not.
func (c *Coalescer) Do(key string, fn func() (any, error)) (any, error) {
	c.mu.Lock()
	if call, ok := c.calls[key]; ok {
		c.mu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}

	call := &coalesceCall{}
	call.wg.Add(1)
	c.calls[key] = call
	c.mu.Unlock()

	call.val, call.err = fn()
	call.wg.Done()

	c.mu.Lock()
	delete(c.calls, key)
	c.mu.Unlock()

	return call.val, call.err
}

// InFlight returns the number of currently coalesced keys.
func (c *Coalescer) InFlight() int {
	c.mu.Lock()
	n := len(c.calls)
	c.mu.Unlock()
	return n
}
