package backpressure

import (
	"strconv"
	"time"

	"github.com/oarkflow/fh"
)

type QueueStatsProvider interface{ Stats() (fh.QueueStats, error) }
type RejectHandler func(fh.Ctx, fh.QueueStats) error

type Config struct {
	Queue         QueueStatsProvider
	MaxPending    int
	MaxProcessing int
	RetryAfter    time.Duration
	Reject        RejectHandler
}

func New(cfg Config) fh.HandlerFunc {
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = 10000
	}
	if cfg.RetryAfter <= 0 {
		cfg.RetryAfter = 5 * time.Second
	}
	if cfg.Reject == nil {
		cfg.Reject = func(c fh.Ctx, st fh.QueueStats) error {
			c.Set("Retry-After", strconv.Itoa(int(cfg.RetryAfter.Seconds())))
			return c.Status(fh.StatusServiceUnavailable).JSON(fh.Map{"error": "queue_backpressure", "pending": st.Pending, "processing": st.Processing})
		}
	}
	return func(c fh.Ctx) error {
		if cfg.Queue == nil {
			return c.Next()
		}
		st, err := cfg.Queue.Stats()
		if err != nil {
			return err
		}
		c.Set("X-Queue-Pending", strconv.Itoa(st.Pending))
		c.Set("X-Queue-Processing", strconv.Itoa(st.Processing))
		if st.Pending >= cfg.MaxPending || (cfg.MaxProcessing > 0 && st.Processing >= cfg.MaxProcessing) {
			return cfg.Reject(c, st)
		}
		return c.Next()
	}
}
