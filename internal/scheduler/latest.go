// Package scheduler contains per-session single-owner scheduling primitives.
package scheduler

// LatestQueue coalesces pending state updates per logical channel/key.
// It does not retract frames that have already been sent.
type LatestQueue struct {
	capacity int
	order    []string
	pending  map[string][]byte
}

func NewLatestQueue(capacity int) *LatestQueue {
	if capacity < 1 {
		panic("capacity must be >0")
	}
	return &LatestQueue{capacity: capacity, pending: make(map[string][]byte, capacity)}
}

// Put overwrites unsent values for an existing key; returns false if full
// and the key is new. Callers retain ownership of the input buffer.
func (q *LatestQueue) Put(key string, value []byte) bool {
	if _, ok := q.pending[key]; !ok {
		if len(q.order) >= q.capacity {
			return false
		}
		q.order = append(q.order, key)
	}
	q.pending[key] = append([]byte(nil), value...)
	return true
}

func (q *LatestQueue) Pop() (string, []byte, bool) {
	if len(q.order) == 0 {
		return "", nil, false
	}
	key := q.order[0]
	q.order[0] = ""
	q.order = q.order[1:]
	value := q.pending[key]
	delete(q.pending, key)
	return key, value, true
}
func (q *LatestQueue) Len() int { return len(q.order) }
