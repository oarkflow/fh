package websocket

import (
	"sync"
	"testing"
)

func TestManagerConnectionLimitIsAtomic(t *testing.T) {
	manager := NewManager()
	manager.MaxConnections = 5
	connections := make([]*Conn, 100)
	for i := range connections {
		connections[i] = &Conn{}
	}

	var wg sync.WaitGroup
	for _, conn := range connections {
		wg.Add(1)
		go func(c *Conn) {
			defer wg.Done()
			_ = manager.TryAdd(c)
		}(conn)
	}
	wg.Wait()
	if got := manager.Count(); got != 5 {
		t.Fatalf("connection count = %d, want 5", got)
	}

	for _, conn := range manager.Snapshot() {
		manager.Remove(conn)
		manager.Remove(conn)
	}
	if got := manager.Count(); got != 0 {
		t.Fatalf("connection count after removal = %d, want 0", got)
	}
}
