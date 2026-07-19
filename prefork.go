package fh

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	preforkChildEnv = "FH_PREFORK_CHILD"
	preforkIndexEnv = "FH_PREFORK_INDEX"
	preforkGenEnv   = "FH_PREFORK_GEN"
)

// PreforkConfig configures App.ListenPrefork's OS-process supervisor. Zero
// values are replaced with sane defaults by normalize.
type PreforkConfig struct {
	// Workers is the number of OS worker processes to run. Defaults to
	// runtime.NumCPU() — parallelism comes from separate processes bound to
	// the same port via SO_REUSEPORT, not from goroutine scheduling alone.
	Workers int
	// WorkerReactors overrides each worker's kernel.Reactors (the existing
	// goroutine-level SO_REUSEPORT accept sharding within one process).
	// Defaults to 1: with Workers OS processes already providing parallelism,
	// stacking reactors on top by default would oversubscribe the machine.
	WorkerReactors int
	// ReadyTimeout bounds how long the master waits for a worker to report a
	// bound, accepting listener before treating startup (or a rollout) as
	// failed.
	ReadyTimeout time.Duration
	// ShutdownTimeout bounds how long a worker gets to drain in-flight
	// requests before the master force-kills it, both during a rolling
	// restart and final shutdown.
	ShutdownTimeout time.Duration
	// RestartBackoffMin/Max bound the exponential backoff applied when
	// respawning a worker that exits unexpectedly, to avoid crash-looping a
	// broken binary.
	RestartBackoffMin time.Duration
	RestartBackoffMax time.Duration
}

// PreforkOption configures a PreforkConfig passed to App.ListenPrefork.
type PreforkOption func(*PreforkConfig)

func WithPreforkWorkers(n int) PreforkOption {
	return func(c *PreforkConfig) { c.Workers = n }
}

func WithPreforkWorkerReactors(n int) PreforkOption {
	return func(c *PreforkConfig) { c.WorkerReactors = n }
}

func WithPreforkReadyTimeout(d time.Duration) PreforkOption {
	return func(c *PreforkConfig) { c.ReadyTimeout = d }
}

func WithPreforkShutdownTimeout(d time.Duration) PreforkOption {
	return func(c *PreforkConfig) { c.ShutdownTimeout = d }
}

func WithPreforkRestartBackoff(min, max time.Duration) PreforkOption {
	return func(c *PreforkConfig) { c.RestartBackoffMin, c.RestartBackoffMax = min, max }
}

func defaultPreforkConfig() PreforkConfig {
	return PreforkConfig{
		Workers:           runtime.NumCPU(),
		WorkerReactors:    1,
		ReadyTimeout:      10 * time.Second,
		ShutdownTimeout:   30 * time.Second,
		RestartBackoffMin: 500 * time.Millisecond,
		RestartBackoffMax: 30 * time.Second,
	}
}

func (c *PreforkConfig) normalize() error {
	d := defaultPreforkConfig()
	if c.Workers <= 0 {
		c.Workers = d.Workers
	}
	if c.WorkerReactors <= 0 {
		c.WorkerReactors = d.WorkerReactors
	}
	if c.ReadyTimeout <= 0 {
		c.ReadyTimeout = d.ReadyTimeout
	}
	if c.ShutdownTimeout <= 0 {
		c.ShutdownTimeout = d.ShutdownTimeout
	}
	if c.RestartBackoffMin <= 0 {
		c.RestartBackoffMin = d.RestartBackoffMin
	}
	if c.RestartBackoffMax <= 0 {
		c.RestartBackoffMax = d.RestartBackoffMax
	}
	if c.RestartBackoffMax < c.RestartBackoffMin {
		return errors.New("fh: PreforkConfig.RestartBackoffMax must be >= RestartBackoffMin")
	}
	return nil
}

func nextPreforkBackoff(current time.Duration, cfg PreforkConfig) time.Duration {
	if current <= 0 {
		return cfg.RestartBackoffMin
	}
	next := current * 2
	if next < current || next > cfg.RestartBackoffMax {
		return cfg.RestartBackoffMax
	}
	return next
}

// ListenPrefork serves addr using a supervisor of Workers OS processes bound
// to the same port via SO_REUSEPORT, instead of a single process. The calling
// binary re-executes itself for each worker, so route registration in main()
// naturally runs again in every worker — call it exactly where you would
// otherwise call Listen or ListenWithGracefulShutdown.
//
// The same mechanism doubles as a zero-downtime restart facility: sending
// SIGHUP to the master process (Unix; use Reload on Windows) spawns a fresh
// generation of workers, waits for them to report a bound listener, then
// gracefully drains and terminates the previous generation — the listening
// port stays accepting connections throughout. SIGINT/SIGTERM to the master
// gracefully stops the whole supervisor.
func (a *App) ListenPrefork(addr string, opts ...PreforkOption) error {
	cfg := defaultPreforkConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.normalize(); err != nil {
		return err
	}

	if os.Getenv(preforkChildEnv) == "1" {
		return a.runPreforkChild(addr, cfg)
	}
	return a.runPreforkMaster(addr, cfg)
}

// Reload triggers a rolling restart of an active ListenPrefork master's
// worker generation, identical to what sending SIGHUP does on Unix. It is
// the documented way to trigger a zero-downtime restart on platforms without
// SIGHUP (Windows), and is available on every platform for programmatic use
// (e.g. from an external control endpoint). It returns an error if this
// process is not currently running as a ListenPrefork master.
func (a *App) Reload() error {
	sup := a.preforkSupervisor.Load()
	if sup == nil {
		return errors.New("fh: Reload requires an active ListenPrefork master in this process")
	}
	go sup.rollingRestart()
	return nil
}

func (a *App) runPreforkChild(addr string, cfg PreforkConfig) error {
	// Prefork requires SO_REUSEPORT so every worker can bind addr
	// concurrently, which only the kernel-assisted transport sets up. If the
	// app wasn't already configured for it (the common case — most callers
	// just add fh.WithPreforkWorkers-style options, not a full KernelConfig),
	// start from the normalized defaults rather than a zero-value
	// KernelConfig{} (whose empty Backend NormalizeConfig never filled in,
	// since it only runs at App-build time and only when Kernel.Enabled was
	// already true then). A caller that did configure Kernel explicitly
	// keeps their own Backend/profile choices; only ReusePort and Reactors
	// are forced.
	if !a.cfg.Kernel.Enabled {
		a.cfg.Kernel = DefaultKernelConfig()
	}
	a.cfg.Kernel.Enabled = true
	a.cfg.Kernel.ReusePort = true
	if cfg.WorkerReactors > 0 {
		a.cfg.Kernel.Reactors = cfg.WorkerReactors
	}
	if f := os.NewFile(3, "fh-prefork-ready"); f != nil {
		a.OnListen(func() error {
			// Best-effort: a write failure here must never block the worker
			// from serving, e.g. when FH_PREFORK_CHILD is set manually
			// without a real fd 3 during local debugging.
			_, _ = f.Write([]byte{1})
			_ = f.Close()
			return nil
		})
	}
	return a.ListenWithGracefulShutdown(addr)
}

// preforkChild tracks one spawned worker OS process.
type preforkChild struct {
	cmd       *exec.Cmd
	index     int
	gen       uint64
	ready     chan struct{}
	readyPipe *os.File
	exited    chan struct{}
	exitErr   error
	killedBy  atomic.Bool // true once the supervisor intentionally terminated it
}

// preforkSupervisor is the master-side process manager. children is guarded
// by mu; generation/restarting/shuttingDown are atomics so the OS-specific
// signal loop, the rolling-restart goroutine, and each worker's crash
// monitor goroutine can run concurrently without racing.
type preforkSupervisor struct {
	app  *App
	addr string
	cfg  PreforkConfig

	mu       sync.Mutex
	children []*preforkChild

	generation   atomic.Uint64
	restarting   atomic.Bool
	shuttingDown atomic.Bool
}

func (a *App) runPreforkMaster(addr string, cfg PreforkConfig) error {
	sup := &preforkSupervisor{app: a, addr: addr, cfg: cfg}
	gen := sup.generation.Add(1)

	children, err := sup.spawnGeneration(gen)
	if err != nil {
		sup.killAll(children)
		return fmt.Errorf("fh: prefork startup failed: %w", err)
	}
	if err := waitChildrenReady(children, cfg.ReadyTimeout); err != nil {
		sup.killAll(children)
		return fmt.Errorf("fh: prefork startup failed: %w", err)
	}

	sup.mu.Lock()
	sup.children = children
	sup.mu.Unlock()
	for _, c := range children {
		go sup.watchAndRespawn(c)
	}

	a.preforkSupervisor.Store(sup)
	defer a.preforkSupervisor.Store(nil)

	a.logger.Info("prefork master ready", "workers", len(children), "generation", gen, "addr", addr)
	return sup.runSignalLoop()
}

func (sup *preforkSupervisor) spawnGeneration(gen uint64) ([]*preforkChild, error) {
	children := make([]*preforkChild, 0, sup.cfg.Workers)
	for i := 0; i < sup.cfg.Workers; i++ {
		c, err := sup.spawnChild(i, gen)
		if err != nil {
			return children, err
		}
		children = append(children, c)
	}
	return children, nil
}

func (sup *preforkSupervisor) spawnChild(index int, gen uint64) (*preforkChild, error) {
	execPath, err := os.Executable()
	if err != nil {
		execPath = os.Args[0]
	}
	r, w, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(execPath, os.Args[1:]...)
	cmd.Env = append(append([]string{}, os.Environ()...),
		preforkChildEnv+"=1",
		fmt.Sprintf("%s=%d", preforkIndexEnv, index),
		fmt.Sprintf("%s=%d", preforkGenEnv, gen),
	)
	cmd.ExtraFiles = []*os.File{w}
	cmd.Stdout, cmd.Stderr, cmd.Stdin = os.Stdout, os.Stderr, os.Stdin
	if err := cmd.Start(); err != nil {
		_ = r.Close()
		_ = w.Close()
		return nil, err
	}
	_ = w.Close() // the child holds its own duplicate of the write end

	c := &preforkChild{cmd: cmd, index: index, gen: gen, ready: make(chan struct{}), readyPipe: r, exited: make(chan struct{})}
	go func() {
		var buf [1]byte
		if n, _ := r.Read(buf[:]); n > 0 {
			close(c.ready)
		}
		_ = r.Close()
	}()
	go func() {
		c.exitErr = cmd.Wait()
		close(c.exited)
	}()
	return c, nil
}

func waitChildrenReady(children []*preforkChild, timeout time.Duration) error {
	deadline := time.After(timeout)
	for _, c := range children {
		select {
		case <-c.ready:
		case <-c.exited:
			return fmt.Errorf("worker %d exited before becoming ready: %w", c.index, c.exitErr)
		case <-deadline:
			return fmt.Errorf("worker %d did not become ready within %s", c.index, timeout)
		}
	}
	return nil
}

func (sup *preforkSupervisor) stopChild(c *preforkChild) {
	c.killedBy.Store(true)
	if c.cmd == nil || c.cmd.Process == nil {
		return
	}
	_ = preforkTerminate(c.cmd.Process)
	timer := time.AfterFunc(sup.cfg.ShutdownTimeout, func() { _ = c.cmd.Process.Kill() })
	<-c.exited
	timer.Stop()
}

func (sup *preforkSupervisor) killAll(children []*preforkChild) {
	var wg sync.WaitGroup
	for _, c := range children {
		wg.Add(1)
		go func(c *preforkChild) {
			defer wg.Done()
			sup.stopChild(c)
		}(c)
	}
	wg.Wait()
}

// watchAndRespawn keeps one worker slot alive across unexpected exits
// (crashes), with exponential backoff. It stops permanently once the
// supervisor deliberately kills the child (stopChild), once the master is
// shutting down entirely, or once c's generation has been superseded by a
// rolling restart — at that point the new generation's own watchAndRespawn
// goroutines (started in rollingRestart) own that responsibility instead.
func (sup *preforkSupervisor) watchAndRespawn(c *preforkChild) {
	var backoff time.Duration
	for {
		<-c.exited
		if c.killedBy.Load() || sup.shuttingDown.Load() {
			return
		}
		if sup.generation.Load() != c.gen {
			return
		}
		backoff = nextPreforkBackoff(backoff, sup.cfg)
		sup.app.logger.Warn("prefork worker exited unexpectedly, restarting",
			"index", c.index, "generation", c.gen, "backoff", backoff, "error", c.exitErr)
		time.Sleep(backoff)
		if sup.shuttingDown.Load() || sup.generation.Load() != c.gen {
			return
		}

		nc, err := sup.spawnChild(c.index, c.gen)
		if err != nil {
			sup.app.logger.Error("prefork worker respawn failed, will retry", "index", c.index, "error", err)
			nc = &preforkChild{index: c.index, gen: c.gen, exited: make(chan struct{})}
			close(nc.exited)
		} else {
			go func() { <-nc.ready }() // drain so the readiness goroutine can exit; result unused here
		}

		sup.mu.Lock()
		for i, existing := range sup.children {
			if existing == c {
				sup.children[i] = nc
				break
			}
		}
		sup.mu.Unlock()
		c = nc
	}
}

// rollingRestart implements the zero-downtime restart: spawn a full new
// generation, wait for it to be ready, swap it in, then drain the previous
// generation. A failure at any point before the swap rolls back to the old
// generation, which keeps serving throughout.
func (sup *preforkSupervisor) rollingRestart() {
	if !sup.restarting.CompareAndSwap(false, true) {
		sup.app.logger.Warn("prefork rolling restart already in progress, ignoring request")
		return
	}
	defer sup.restarting.Store(false)

	prevGen := sup.generation.Load()
	newGen := sup.generation.Add(1)
	sup.app.logger.Info("prefork rolling restart starting", "generation", newGen)

	rollback := func(err error) {
		sup.app.logger.Error("prefork rolling restart failed, rolling back", "generation", newGen, "error", err)
		// Restore the generation counter so the still-serving previous
		// generation's crash-respawn watchers keep matching sup.generation.
		// A crash in that previous generation landing in the narrow window
		// between the Add above and this Store will miss its own restart;
		// this is an accepted, rare edge case rather than a full two-phase
		// generation-commit protocol.
		sup.generation.Store(prevGen)
	}

	newChildren, err := sup.spawnGeneration(newGen)
	if err != nil {
		sup.killAll(newChildren)
		rollback(err)
		return
	}
	if err := waitChildrenReady(newChildren, sup.cfg.ReadyTimeout); err != nil {
		sup.killAll(newChildren)
		rollback(err)
		return
	}

	for _, c := range newChildren {
		go sup.watchAndRespawn(c)
	}

	sup.mu.Lock()
	oldChildren := sup.children
	sup.children = newChildren
	sup.mu.Unlock()

	sup.app.logger.Info("prefork rolling restart draining previous generation", "generation", prevGen)
	sup.killAll(oldChildren)
	sup.app.logger.Info("prefork rolling restart complete", "generation", newGen)
}

func (sup *preforkSupervisor) shutdown() {
	sup.shuttingDown.Store(true)
	sup.mu.Lock()
	children := sup.children
	sup.mu.Unlock()
	sup.app.logger.Info("prefork master shutting down", "workers", len(children))
	sup.killAll(children)
}
