package fh

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBudgetChildAddsDeadlineWithoutParentDeadline(t *testing.T) {
	child := NewBudget(BudgetConfig{}).Child(40*time.Millisecond, BudgetSplit{})
	if child.Deadline.IsZero() {
		t.Fatal("child with a finite duration has no deadline")
	}
	if remaining := child.Remaining(); remaining <= 0 || remaining > 40*time.Millisecond {
		t.Fatalf("child remaining = %s, want >0 and <=40ms", remaining)
	}
	if child.Expired() {
		t.Fatal("new child budget is already expired")
	}
}

func TestBudgetChildDeadlineCannotExceedParent(t *testing.T) {
	parent := NewBudget(BudgetConfig{Deadline: 40 * time.Millisecond})
	child := parent.Child(time.Second, BudgetSplit{})
	if child.Deadline.IsZero() || child.Deadline.After(parent.Deadline) {
		t.Fatalf("child deadline %v exceeds parent deadline %v", child.Deadline, parent.Deadline)
	}
}

func TestBudgetChildContextCarriesChildDeadline(t *testing.T) {
	parent := NewBudget(BudgetConfig{Deadline: time.Second})
	child := parent.Child(25*time.Millisecond, BudgetSplit{})
	ctx, cancel := child.Context(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok || deadline.After(child.Deadline) {
		t.Fatalf("context deadline = %v (ok=%v), child deadline = %v", deadline, ok, child.Deadline)
	}
	if BudgetFromContext(ctx) != child {
		t.Fatal("child budget was not attached to its context")
	}
}

func TestBudgetExpiredAtDeadline(t *testing.T) {
	b := &Budget{Deadline: time.Now().Add(-time.Nanosecond)}
	if !b.Expired() || b.Remaining() != 0 {
		t.Fatalf("expired budget reports expired=%v remaining=%s", b.Expired(), b.Remaining())
	}
}

func TestBudgetMiddlewarePropagatesDeadlineToRequestContext(t *testing.T) {
	app := New(WithStartupBannerDisabled(true))
	app.Use(BudgetMiddleware(BudgetConfig{Deadline: time.Second}))
	app.Get("/", func(c Ctx) error {
		_, ok := c.Context().Deadline()
		return c.SendString(strconv.FormatBool(ok))
	})
	req, _ := http.NewRequest(http.MethodGet, "http://localhost/", nil)
	resp, err := app.Test(req, 1000)
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if resp.StatusCode != StatusOK || string(body) != "true" {
		t.Fatalf("budget middleware response = %d %q; want 200 true", resp.StatusCode, body)
	}
}

func TestBudgetReservesUpstreamCallsConcurrently(t *testing.T) {
	b := NewBudget(BudgetConfig{MaxUpstreamCalls: 10})
	var accepted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.ReserveUpstream() {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := accepted.Load(); got != 10 || b.CanUpstream() {
		t.Fatalf("accepted %d upstream calls; CanUpstream=%v, want 10 and false", got, b.CanUpstream())
	}
}

func TestBudgetLiteralEnforcesUpstreamLimit(t *testing.T) {
	b := &Budget{MaxUpstreamCalls: 1}
	if !b.ReserveUpstream() || b.ReserveUpstream() || b.CanUpstream() {
		t.Fatal("budget initialized from exported fields did not enforce its upstream limit")
	}
}

func TestChildUpstreamBudgetEnforcesChildAndParentCaps(t *testing.T) {
	parent := NewBudget(BudgetConfig{MaxUpstreamCalls: 5})
	child := parent.Child(0, BudgetSplit{UpstreamFraction: 0.5})
	if child.MaxUpstreamCalls != 2 {
		t.Fatalf("child upstream limit = %d, want floor(5*0.5)=2", child.MaxUpstreamCalls)
	}
	childFirst := child.ReserveUpstream()
	childSecond := child.ReserveUpstream()
	if !childFirst || !childSecond || child.CanUpstream() {
		t.Fatal("child budget did not enforce its two-call allowance")
	}

	zero := parent.Child(0, BudgetSplit{})
	if zero.CanUpstream() || zero.ReserveUpstream() {
		t.Fatal("zero-fraction child unexpectedly allowed an upstream call")
	}
	parentFirst := parent.ReserveUpstream()
	parentSecond := parent.ReserveUpstream()
	parentThird := parent.ReserveUpstream()
	if !parentFirst || !parentSecond || !parentThird {
		t.Fatal("parent did not retain its remaining upstream allowance")
	}
	if parent.ReserveUpstream() || parent.CanUpstream() {
		t.Fatal("parent exceeded its aggregate upstream-call cap")
	}
}
