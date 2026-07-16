package ratelimiter

import (
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/oarkflow/fh"
)

type AdaptiveConfig struct {
	BaseConfig      Config
	HighWaterMark   int64
	ScaleDownFactor float64
	ScaleUpFactor   float64
	MinLimit        int
	MaxLimit        int
	CheckInterval   time.Duration
}

func NewAdaptive(cfg AdaptiveConfig) (fh.HandlerFunc, func()) {
	if cfg.BaseConfig.Store == nil {
		cfg.BaseConfig.Store = NewMemoryStore(256)
	}
	if cfg.BaseConfig.Max <= 0 {
		cfg.BaseConfig.Max = 100
	}
	if cfg.BaseConfig.Window <= 0 {
		cfg.BaseConfig.Window = time.Minute
	}
	if cfg.BaseConfig.KeyFunc == nil {
		cfg.BaseConfig.KeyFunc = func(ctx fh.Ctx) string { return ctx.IP() }
	}
	if cfg.BaseConfig.LimitReached == nil {
		cfg.BaseConfig.LimitReached = DefaultLimitReachedHandler
	}

	if cfg.HighWaterMark <= 0 {
		cfg.HighWaterMark = 1000
	}
	if cfg.ScaleDownFactor <= 0 {
		cfg.ScaleDownFactor = 0.5
	}
	if cfg.ScaleUpFactor <= 0 {
		cfg.ScaleUpFactor = 1.1
	}
	if cfg.MinLimit <= 0 {
		cfg.MinLimit = 10
	}
	if cfg.MaxLimit <= 0 {
		cfg.MaxLimit = cfg.BaseConfig.Max * 10
	}
	if cfg.MaxLimit < cfg.MinLimit {
		cfg.MaxLimit = cfg.MinLimit
	}
	if cfg.CheckInterval <= 0 {
		cfg.CheckInterval = time.Second
	}

	var currentLimit atomic.Int64
	currentLimit.Store(int64(cfg.BaseConfig.Max))

	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(cfg.CheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				load := runtime.NumGoroutine()
				cur := currentLimit.Load()
				if int64(load) > cfg.HighWaterMark {
					newLimit := int64(float64(cur) * cfg.ScaleDownFactor)
					if newLimit < int64(cfg.MinLimit) {
						newLimit = int64(cfg.MinLimit)
					}
					currentLimit.Store(newLimit)
				} else if int64(load) < cfg.HighWaterMark/2 {
					newLimit := int64(float64(cur) * cfg.ScaleUpFactor)
					if newLimit > int64(cfg.MaxLimit) {
						newLimit = int64(cfg.MaxLimit)
					}
					currentLimit.Store(newLimit)
				}
			case <-stop:
				return
			}
		}
	}()

	var shutdownOnce sync.Once
	shutdown := func() { shutdownOnce.Do(func() { close(stop) }) }

	middleware := func(ctx fh.Ctx) error {
		if cfg.BaseConfig.Skip != nil && cfg.BaseConfig.Skip(ctx) {
			return ctx.Next()
		}

		key := cfg.BaseConfig.KeyFunc(ctx)
		if key == "" {
			key = "unknown"
		}

		limit := int(currentLimit.Load())
		now := time.Now()
		result, err := cfg.BaseConfig.Store.Allow(key, limit, cfg.BaseConfig.Window, now)
		if err != nil {
			return err
		}

		if cfg.BaseConfig.SendHeaders {
			ctx.Set(HeaderLimit, strconv.Itoa(limit))
			var buf [32]byte
			rem := result.Remaining
			if rem < 0 {
				rem = 0
			}
			ctx.Set(HeaderRemaining, string(strconv.AppendInt(buf[:0], int64(rem), 10)))
			resetUnix := result.ResetAt.Unix()
			ctx.Set(HeaderReset, string(strconv.AppendInt(buf[:0], resetUnix, 10)))
		}

		if !result.Allowed {
			return cfg.BaseConfig.LimitReached(ctx, result)
		}

		return ctx.Next()
	}

	return middleware, shutdown
}

type LoadAwareLimiter struct {
	mu          sync.RWMutex
	baseLimit   int
	highWater   int64
	scaleDown   float64
	scaleUp     float64
	minLimit    int
	maxLimit    int
	store       Store
	window      time.Duration
	keyFunc     KeyFunc
	sendHeaders bool
	reached     LimitReachedHandler
	stop        chan struct{}
	once        sync.Once
}

func NewLoadAware(baseCfg Config, highWater int64) *LoadAwareLimiter {
	if baseCfg.Store == nil {
		baseCfg.Store = NewMemoryStore(256)
	}
	if baseCfg.Max <= 0 {
		baseCfg.Max = 100
	}
	l := &LoadAwareLimiter{
		baseLimit:   baseCfg.Max,
		highWater:   highWater,
		scaleDown:   0.5,
		scaleUp:     1.1,
		minLimit:    baseCfg.Max / 10,
		maxLimit:    baseCfg.Max * 10,
		store:       baseCfg.Store,
		window:      baseCfg.Window,
		keyFunc:     baseCfg.KeyFunc,
		sendHeaders: baseCfg.SendHeaders,
		reached:     baseCfg.LimitReached,
		stop:        make(chan struct{}),
	}
	if l.minLimit < 1 {
		l.minLimit = 1
	}
	go l.adjustLoop()
	return l
}

func (l *LoadAwareLimiter) adjustLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			load := runtime.NumGoroutine()
			l.mu.Lock()
			if int64(load) > l.highWater {
				newLimit := int(float64(l.baseLimit) * l.scaleDown)
				if newLimit < l.minLimit {
					newLimit = l.minLimit
				}
				l.baseLimit = newLimit
			} else if int64(load) < l.highWater/2 {
				newLimit := int(float64(l.baseLimit) * l.scaleUp)
				if newLimit > l.maxLimit {
					newLimit = l.maxLimit
				}
				l.baseLimit = newLimit
			}
			l.mu.Unlock()
		case <-l.stop:
			return
		}
	}
}

func (l *LoadAwareLimiter) Middleware() fh.HandlerFunc {
	return func(ctx fh.Ctx) error {
		key := l.keyFunc(ctx)
		if key == "" {
			key = "unknown"
		}
		l.mu.RLock()
		limit := l.baseLimit
		l.mu.RUnlock()

		now := time.Now()
		result, err := l.store.Allow(key, limit, l.window, now)
		if err != nil {
			return err
		}

		if l.sendHeaders {
			ctx.Set(HeaderLimit, strconv.Itoa(limit))
			var buf [32]byte
			rem := result.Remaining
			if rem < 0 {
				rem = 0
			}
			ctx.Set(HeaderRemaining, string(strconv.AppendInt(buf[:0], int64(rem), 10)))
			resetUnix := result.ResetAt.Unix()
			ctx.Set(HeaderReset, string(strconv.AppendInt(buf[:0], resetUnix, 10)))
		}

		if !result.Allowed {
			return l.reached(ctx, result)
		}

		return ctx.Next()
	}
}

func (l *LoadAwareLimiter) CurrentLimit() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.baseLimit
}

func (l *LoadAwareLimiter) SetBaseLimit(limit int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.baseLimit = limit
}

func (l *LoadAwareLimiter) Stop() {
	l.once.Do(func() { close(l.stop) })
}
