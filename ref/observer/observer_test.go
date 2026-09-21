package observer_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/ref/graph"
	"github.com/oarkflow/fh/ref/observer"
)

type mockObserver struct {
	observer.Noop
	startedCount atomic.Int32
	shouldPanic  bool
}

func (m *mockObserver) NodeStarted(info graph.NodeInfo) {
	if m.shouldPanic {
		panic("observer internal failure")
	}
	m.startedCount.Add(1)
}

func TestPanicIsolation(t *testing.T) {
	panicking := &mockObserver{shouldPanic: true}
	normal := &mockObserver{shouldPanic: false}

	co := observer.NewCompositeObserver(
		nil,
		observer.TieredObserver{Observer: panicking, Tier: observer.Critical},
		observer.TieredObserver{Observer: normal, Tier: observer.Critical},
	)

	// Calling NodeStarted should not crash despite panicking observer
	co.NodeStarted(graph.NodeInfo{ID: 1, Name: "TestNode", Kind: graph.PureNode})

	if normal.startedCount.Load() != 1 {
		t.Errorf("normal observer should have executed despite other observer panic")
	}
}

func TestAsyncDispatcher(t *testing.T) {
	disp := observer.NewAsyncDispatcher(10)
	defer disp.Close()

	var count atomic.Int32
	for i := 0; i < 5; i++ {
		disp.Send(func() {
			count.Add(1)
		})
	}

	time.Sleep(50 * time.Millisecond)
	if count.Load() != 5 {
		t.Errorf("expected 5 events executed, got %d", count.Load())
	}
}
