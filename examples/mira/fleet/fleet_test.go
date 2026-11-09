package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mira/devpki"
	"github.com/oarkflow/fh/mira/fleet"
)

func call(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// TestOperatorAPI drives the real HTTP API against real MIRA devices.
func TestOperatorAPI(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ca, _ := devpki.New()
	tlsCfg, _ := ca.ServerTLS()
	app := fh.New()
	defer app.Shutdown()
	hub := NewHub()
	Routes(app, hub)
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go app.Serve(ln)
	server, err := app.ListenMIRATLS("127.0.0.1:0", "127.0.0.1:0", tlsCfg, fh.MIRAOptions{MaxChannels: 4, MaxMessage: fleet.MaxCommand})
	if err != nil {
		t.Fatal(err)
	}
	server.SetChannelAuthorizer(hub.Authorize)
	go app.ServeMIRA(ctx, server, hub.Handle)
	for i := 1; i <= 2; i++ {
		cfg, _ := ca.ClientTLS(fmt.Sprintf("device-%d", i))
		dev := NewThermostat()
		go (&fleet.Agent{TLS: cfg, TCPAddr: server.TLSAddr().String(), UDPAddr: server.Addr().String(),
			Interval: 20 * time.Millisecond, Telemetry: dev.Snapshot, OnCommand: dev.Apply}).Run(ctx)
	}
	base := "http://" + ln.Addr().String()
	deadline := time.Now().Add(10 * time.Second)
	for len(hub.Devices()) < 2 || len(hub.Devices()[1].Telemetry) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("devices never registered")
		}
		time.Sleep(20 * time.Millisecond)
	}
	code, body := call(t, "POST", base+"/fleet/device-2/commands", `{"op":"set_setpoint","value":30}`)
	if code != 200 || !strings.Contains(body, `"processed":true`) {
		t.Fatalf("command: %d %s", code, body)
	}
	for {
		_, body = call(t, "GET", base+"/fleet", "")
		if strings.Contains(body, `"setpoint":30`) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("telemetry never showed the new setpoint: %s", body)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if code, _ = call(t, "POST", base+"/fleet/device-99/commands", `{"op":"set_setpoint","value":1}`); code != 404 {
		t.Fatalf("offline device: want 404, got %d", code)
	}
	if code, _ = call(t, "POST", base+"/fleet/device-1/commands", `{}`); code != 400 {
		t.Fatalf("bad body: want 400, got %d", code)
	}
	if code, _ = call(t, "POST", base+"/fleet/device-1/commands", `{"op":"explode"}`); code != 504 {
		t.Fatalf("unsupported command must not be confirmed: got %d", code)
	}
}
