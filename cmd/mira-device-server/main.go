// mira-device-server is a strict, native-UDP sample with per-device PSKs,
// per-channel authorization, credential reload and live device revocation.
// This remains an unaudited experimental protocol; do not publicly expose it.
package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oarkflow/fh/durable"
	"github.com/oarkflow/fh/journal"
	"github.com/oarkflow/fh/mira"
	"github.com/oarkflow/fh/protocol"
)

type deviceSpec struct {
	Key             string `json:"key"`
	AllowReliable   bool   `json:"allow_reliable"`
	AllowBestEffort bool   `json:"allow_best_effort"`
}
type configFile struct {
	Devices map[string]deviceSpec `json:"devices"`
}
type device struct {
	key                  []byte
	reliable, bestEffort bool
}
type config struct{ devices map[string]device }

func load(path string) (*config, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("credential file must be a regular file accessible only by its owner (chmod 600): %s", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(io.LimitReader(f, 1<<20))
	dec.DisallowUnknownFields()
	var raw configFile
	if err = dec.Decode(&raw); err != nil {
		return nil, err
	}
	var extra any
	if e := dec.Decode(&extra); e != io.EOF {
		return nil, errors.New("trailing config data")
	}
	if len(raw.Devices) < 1 || len(raw.Devices) > 5000 {
		return nil, errors.New("devices must contain 1..5000 entries")
	}
	c := &config{devices: make(map[string]device, len(raw.Devices))}
	for id, spec := range raw.Devices {
		if len(id) < 1 || len(id) > 64 {
			return nil, fmt.Errorf("invalid device identity length")
		}
		for _, r := range id {
			if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_' && r != '.' && r != '@' {
				return nil, errors.New("invalid device identity")
			}
		}
		key, err := hex.DecodeString(spec.Key)
		if err != nil || len(key) != 32 {
			return nil, fmt.Errorf("device %q needs 64 hex chars of random key", id)
		}
		c.devices[id] = device{key: key, reliable: spec.AllowReliable, bestEffort: spec.AllowBestEffort}
	}
	return c, nil
}

func handle(ctx context.Context, s *mira.Session, st *durable.Store) {
	defer s.Close(context.Background())
	for {
		ch, err := s.AcceptChannel(ctx)
		if err != nil {
			return
		}
		go func(c protocol.Channel) {
			defer c.Close(context.Background())
			if st != nil {
				if e := journal.ServeChannel(ctx, c, protocol.Principal{Subject: s.Identity(), Device: s.Identity()}, st); e != nil && ctx.Err() == nil {
					log.Printf("journal session ended: %v", e)
				}
				return
			}
			for {
				msg, err := c.Receive(ctx)
				if err != nil {
					return
				}
				if _, err = c.Send(ctx, msg.Payload); err != nil {
					return
				}
			}
		}(ch)
	}
}

func main() {
	address := flag.String("listen", "127.0.0.1:7447", "UDP bind address")
	configPath := flag.String("config", "", "owner-only JSON credential file")
	walPath := flag.String("wal", "", "optional durable journal WAL path; enables journal profile instead of echo")
	metricsAddr := flag.String("metrics", "", "optional loopback-only HTTP metrics listener, e.g. 127.0.0.1:9097")
	flag.Parse()
	if *configPath == "" {
		log.Fatal("-config is required")
	}
	initial, err := load(*configPath)
	if err != nil {
		log.Fatal(err)
	}
	var current atomic.Pointer[config]
	current.Store(initial)
	srv, err := mira.ListenWithResolver(*address, func(id string) ([]byte, bool) {
		item, ok := current.Load().devices[id]
		return item.key, ok
	}, mira.Options{MaxSessions: 500, MaxSessionsPerIdentity: 4, MaxInboundBytesPerSecond: 4 << 20, MaxChannels: 8, MaxMessage: 256 << 10, IdleTimeout: 90 * time.Second})
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Close()
	var st *durable.Store
	if *walPath != "" {
		st, err = durable.Open(*walPath, 128<<20)
		if err != nil {
			log.Fatal(err)
		}
		defer st.Close()
	}
	if *metricsAddr != "" {
		host, _, err := net.SplitHostPort(*metricsAddr)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			log.Fatal("-metrics must use a loopback IP literal")
		}
		mux := http.NewServeMux()
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "GET" {
				http.Error(w, "method not allowed", 405)
				return
			}
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_ = srv.WritePrometheus(w)
		})
		monitor := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 3 * time.Second}
		defer monitor.Close()
		go func() {
			if e := monitor.ListenAndServe(); e != nil && e != http.ErrServerClosed {
				log.Printf("metrics listener stopped: %v", e)
			}
		}()
	}
	srv.SetChannelAuthorizer(func(identity string, cfg protocol.ChannelConfig) bool {
		item, ok := current.Load().devices[identity]
		if !ok {
			return false
		}
		if cfg.Delivery == protocol.ReliableOrdered || cfg.Delivery == protocol.ReliableUnordered || cfg.Delivery == protocol.Bulk {
			return item.reliable
		}
		return item.bestEffort
	})
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	defer signal.Stop(reload)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-reload:
				next, e := load(*configPath)
				if e != nil {
					log.Printf("credential reload rejected: %v", e)
					continue
				}
				previous := current.Swap(next)
				// Revoke deleted/rotated identities and reallow valid entries.
				for id, old := range previous.devices {
					fresh, ok := next.devices[id]
					if !ok || !bytes.Equal(fresh.key, old.key) {
						srv.RevokeIdentity(id)
					}
				}
				for id := range next.devices {
					srv.AllowIdentity(id)
				}
				log.Printf("credentials reloaded: %d configured devices", len(next.devices))
			}
		}
	}()
	log.Printf("experimental MIRA-T/1 UDP server on %s: %d authenticated devices", srv.Addr(), len(initial.devices))
	for {
		sess, e := srv.Accept(ctx)
		if e != nil {
			return
		}
		go handle(ctx, sess, st)
	}
}
