package fleet_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/mira/fleet"
	"github.com/oarkflow/fh/protocol"
)

type env struct {
	ca  *devpki.CA
	srv *mira.SecureServer
	hub *fleet.Hub
	ctx context.Context
}

func start(t *testing.T) *env {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ca, err := devpki.New()
	if err != nil {
		t.Fatal(err)
	}
	tlsCfg, err := ca.ServerTLS()
	if err != nil {
		t.Fatal(err)
	}
	srv, err := mira.ListenTLS("127.0.0.1:0", "127.0.0.1:0", tlsCfg, mira.Options{MaxChannels: 4, MaxMessage: fleet.MaxCommand})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	hub := fleet.NewHub(func(id string) bool { return strings.HasPrefix(id, "dev-") })
	srv.SetChannelAuthorizer(hub.Authorize)
	go func() {
		for {
			s, err := srv.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				for {
					ch, err := s.AcceptChannel(ctx)
					if err != nil {
						return
					}
					go hub.Handle(ctx, s.Identity(), ch)
				}
			}()
		}
	}()
	return &env{ca: ca, srv: srv, hub: hub, ctx: ctx}
}

func (e *env) agent(t *testing.T, id string, on func(context.Context, []byte) error) *fleet.Agent {
	t.Helper()
	cfg, err := e.ca.ClientTLS(id)
	if err != nil {
		t.Fatal(err)
	}
	a := &fleet.Agent{
		TLS: cfg, TCPAddr: e.srv.TLSAddr().String(), UDPAddr: e.srv.Addr().String(),
		Interval:  10 * time.Millisecond,
		Telemetry: func() []byte { return []byte(id) },
		OnCommand: on,
	}
	go a.Run(e.ctx)
	return a
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestDispatchConfirmsAfterDeviceApplies(t *testing.T) {
	e := start(t)
	var applied atomic.Value
	e.agent(t, "dev-1", func(_ context.Context, c []byte) error { applied.Store(string(c)); return nil })
	waitFor(t, "registration with telemetry", func() bool {
		d := e.hub.Devices()
		return len(d) == 1 && string(d[0].Telemetry) == "dev-1"
	})
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	if _, err := e.hub.Dispatch(ctx, "dev-1", []byte("reboot")); err != nil {
		t.Fatal(err)
	}
	// AckProcessed came back, so the effect must already be visible.
	if applied.Load() != "reboot" {
		t.Fatalf("Dispatch returned before the device applied the command: %v", applied.Load())
	}
}

func TestFailedCommandIsNotConfirmed(t *testing.T) {
	e := start(t)
	results := make(chan string, 1)
	e.hub.OnResult = func(_ string, p []byte) { results <- string(p) }
	e.agent(t, "dev-1", func(context.Context, []byte) error { return errors.New("actuator jammed") })
	waitFor(t, "registration", func() bool { return len(e.hub.Devices()) == 1 })
	ctx, cancel := context.WithTimeout(e.ctx, 500*time.Millisecond)
	defer cancel()
	if _, err := e.hub.Dispatch(ctx, "dev-1", []byte("x")); err == nil {
		t.Fatal("a failed command must not yield AckProcessed")
	}
	select {
	case r := <-results:
		if !strings.Contains(r, "actuator jammed") {
			t.Fatalf("result %q", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("device result never reported")
	}
}

func TestPolicyAndOffline(t *testing.T) {
	e := start(t)
	e.agent(t, "intruder", func(context.Context, []byte) error { return nil })
	time.Sleep(500 * time.Millisecond)
	if n := len(e.hub.Devices()); n != 0 {
		t.Fatalf("unauthorized identity registered (%d devices)", n)
	}
	if _, err := e.hub.Dispatch(e.ctx, "dev-404", nil); !errors.Is(err, fleet.ErrOffline) {
		t.Fatalf("got %v, want ErrOffline", err)
	}
	if fleet.NewHub(nil).Authorize("dev-1", protocol.ChannelConfig{Delivery: protocol.LatestState, MaxMessage: 100}) {
		t.Fatal("nil Allow must refuse everyone")
	}
}

func TestReconnectReplacesRegistration(t *testing.T) {
	e := start(t)
	var n atomic.Int32
	a := e.agent(t, "dev-1", func(context.Context, []byte) error { n.Add(1); return nil })
	_ = a
	waitFor(t, "registration", func() bool { return len(e.hub.Devices()) == 1 })
	e.srv.RevokeIdentity("dev-1") // server drops the session
	waitFor(t, "deregistration", func() bool { return len(e.hub.Devices()) == 0 })
	e.srv.AllowIdentity("dev-1")
	waitFor(t, "agent reconnect", func() bool { return len(e.hub.Devices()) == 1 })
	ctx, cancel := context.WithTimeout(e.ctx, 5*time.Second)
	defer cancel()
	if _, err := e.hub.Dispatch(ctx, "dev-1", []byte("again")); err != nil || n.Load() != 1 {
		t.Fatalf("dispatch after reconnect: err=%v applied=%d", err, n.Load())
	}
}
