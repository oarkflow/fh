package fh_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/oarkflow/fh"
)

func TestHealthStatusConcurrentCalls(t *testing.T) {
	app := fh.New()
	for i := 0; i < 16; i++ {
		app.AddHealthCheck("check-"+string(rune('a'+i)), time.Second, func(context.Context) error {
			return errors.New("down")
		})
	}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, results := app.HealthStatus(context.Background())
			if ok || len(results) != 16 {
				t.Errorf("HealthStatus() = (%v, %d results), want false and 16 results", ok, len(results))
			}
		}()
	}
	wg.Wait()
}
