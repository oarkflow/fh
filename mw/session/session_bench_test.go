package session

import (
	"context"
	"sync"
	"testing"
)

func BenchmarkSessionShardLockUncontended(b *testing.B) {
	var lock sessionShardLock
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := lock.LockContext(ctx, 0); err != nil {
			b.Fatal(err)
		}
		lock.Unlock()
	}
}

func BenchmarkMutexUncontended(b *testing.B) {
	var lock sync.Mutex
	var counter uint64
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		lock.Lock()
		counter++
		lock.Unlock()
	}
	if counter != uint64(b.N) {
		b.Fatalf("counter = %d, want %d", counter, b.N)
	}
}
