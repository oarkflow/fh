package mira

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestAutomaticPathMTUDiscoveryAndOptOut(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "enabled", true: "disabled"}[disabled], func(t *testing.T) {
			secret := bytes.Repeat([]byte{91}, 32)
			options := Options{DisableAutoMTU: disabled}
			srv, err := Listen("127.0.0.1:0", secret, options)
			if err != nil {
				t.Fatal(err)
			}
			defer srv.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
			defer cancel()
			cli, err := Dial(ctx, srv.Addr().String(), secret, options)
			if err != nil {
				t.Fatal(err)
			}
			defer cli.Close(context.Background())
			peer, err := srv.Accept(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer peer.Close(context.Background())
			deadline := time.Now().Add(4500 * time.Millisecond)
			got := 1200
			for time.Now().Before(deadline) {
				stat, err := cli.Stats(ctx)
				if err != nil {
					t.Fatal(err)
				}
				got = stat.PathMTU
				if got > 1200 {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if disabled && got != 1200 {
				t.Fatalf("disabled but MTU %d", got)
			}
			if !disabled && got <= 1200 {
				t.Fatalf("automatic probing failed, got %d", got)
			}
		})
	}
}

func TestResetPathMTUForgetsOutstandingProbe(t *testing.T) {
	s := &Session{pathMTU: 1400, nextMTUProbe: time.Now(), pending: map[uint64]*sentPacket{}, control: []txFrame{}, mtuProbeOutstanding: true, mtuProbeTarget: 1452}
	s.pending[8] = &sentPacket{frame: txFrame{probeMTU: 1452}, size: 1452}
	s.inFlightBytes = 1452
	s.resetPathMTU()
	if s.pathMTU != 1200 || s.mtuProbeOutstanding || s.inFlightBytes != 0 || len(s.pending) != 0 {
		t.Fatalf("state invalid: %+v", s)
	}
}
