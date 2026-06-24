package lifecycle

import (
	"errors"

	"github.com/oarkflow/fh"
)

type Hooks struct {
	OnRequestStart  func(fh.Ctx) error
	OnBeforeHandler func(fh.Ctx) error
	OnAfterHandler  func(fh.Ctx) error
	OnError         func(fh.Ctx, error) error
	OnRequestEnd    func(fh.Ctx, error) error
}

type Config struct {
	Hooks Hooks

	// RunAfterOnError controls whether OnAfterHandler runs after c.Next()
	// even when c.Next() returned an error.
	RunAfterOnError bool

	// SwallowErrorOnHandled controls whether OnError returning nil means
	// the original handler error is considered handled.
	SwallowErrorOnHandled bool
}

func New(h Hooks) fh.HandlerFunc {
	return NewWithConfig(Config{Hooks: h})
}

func NewWithConfig(cfg Config) fh.HandlerFunc {
	h := cfg.Hooks

	return func(c fh.Ctx) (err error) {
		defer func() {
			if h.OnRequestEnd != nil {
				endErr := h.OnRequestEnd(c, err)
				if endErr != nil {
					err = joinErr(err, endErr)
				}
			}
		}()

		if h.OnRequestStart != nil {
			if startErr := h.OnRequestStart(c); startErr != nil {
				err = startErr
				return err
			}
		}

		if h.OnBeforeHandler != nil {
			if beforeErr := h.OnBeforeHandler(c); beforeErr != nil {
				err = beforeErr
				return err
			}
		}

		err = c.Next()

		if err != nil {
			if h.OnError != nil {
				onErr := h.OnError(c, err)
				if onErr != nil {
					err = joinErr(err, onErr)
					return err
				}

				if cfg.SwallowErrorOnHandled {
					err = nil
				}
			}
		}

		if h.OnAfterHandler != nil {
			if err == nil || cfg.RunAfterOnError {
				afterErr := h.OnAfterHandler(c)
				if afterErr != nil {
					err = joinErr(err, afterErr)
					return err
				}
			}
		}

		return err
	}
}

func joinErr(a, b error) error {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	default:
		return errors.Join(a, b)
	}
}
