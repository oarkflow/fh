package fh

import (
	"errors"
	"testing"
)

func TestBufferedResponseBudgetRejectsAndReleases(t *testing.T) {
	app := New(WithMaxBufferedResponseBytes(4))
	ctx := acquireCtx(nil, app)
	defer releaseCtx(ctx)
	if err := ctx.reserveResponseBody(3); err != nil {
		t.Fatal(err)
	}
	if got := app.bufferedResponseBytes.Load(); got != 3 {
		t.Fatalf("reserved bytes = %d, want 3", got)
	}
	if err := ctx.reserveResponseBody(5); !errors.Is(err, ErrResponseBodyTooLarge) {
		t.Fatalf("reserve beyond limit = %v", err)
	}
	if got := app.bodyBudgetRejections.Load(); got != 1 {
		t.Fatalf("budget rejections = %d, want 1", got)
	}
	ctx.releaseResponseBody()
	if got := app.bufferedResponseBytes.Load(); got != 0 {
		t.Fatalf("reserved bytes after release = %d, want 0", got)
	}
}
