package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/mira/fleet"
)

// Application-level message shapes. The transport (mira/fleet) carries opaque
// bytes; JSON is this example's choice.
type Command struct {
	Op    string  `json:"op"`
	Value float64 `json:"value"`
}

type Telemetry struct {
	Seq      uint64  `json:"seq"`
	TempC    float64 `json:"temp_c"`
	Setpoint float64 `json:"setpoint"`
}

// NewHub applies this app's identity policy: certificates named device-*.
func NewHub() *fleet.Hub {
	return fleet.NewHub(func(id string) bool { return strings.HasPrefix(id, "device-") })
}

// Routes is the operator HTTP API, served by the same fh App as the transport.
func Routes(app *fh.App, hub *fleet.Hub) {
	app.Get("/healthz", func(c fh.Ctx) error { return c.SendString("ok") })
	app.Get("/fleet", func(c fh.Ctx) error {
		type row struct {
			Device    string    `json:"device"`
			Telemetry Telemetry `json:"telemetry"`
			UpdatedAt time.Time `json:"updated_at"`
		}
		rows := []row{}
		for _, d := range hub.Devices() {
			r := row{Device: d.ID, UpdatedAt: d.UpdatedAt}
			_ = json.Unmarshal(d.Telemetry, &r.Telemetry) // malformed snapshots show as zero
			rows = append(rows, r)
		}
		return c.JSON(map[string]any{"devices": rows})
	})
	app.Post("/fleet/:id/commands", func(c fh.Ctx) error {
		var cmd Command
		if err := c.BodyParser(&cmd); err != nil || cmd.Op == "" {
			return c.Status(400).JSON(map[string]string{"error": `body must be {"op":...,"value":...}`})
		}
		body, _ := json.Marshal(cmd)
		ctx, cancel := context.WithTimeout(c.Context(), 3*time.Second)
		defer cancel()
		id, err := hub.Dispatch(ctx, c.Param("id"), body)
		switch {
		case errors.Is(err, fleet.ErrOffline):
			return c.Status(404).JSON(map[string]string{"error": "device offline"})
		case err != nil:
			return fh.WrapHTTPError(err, fh.StatusGatewayTimeout, "DEVICE_COMMAND_FAILED", "Device did not confirm the command")
		}
		return c.JSON(map[string]any{"message_id": id, "processed": true})
	})
}

// Thermostat is a simulated device whose state converges on its setpoint.
type Thermostat struct {
	temp, setpoint float64
	seq            uint64
}

func NewThermostat() *Thermostat { return &Thermostat{temp: 20, setpoint: 20} }

func (t *Thermostat) Snapshot() []byte {
	t.temp += (t.setpoint - t.temp) * 0.2
	t.seq++
	b, _ := json.Marshal(Telemetry{Seq: t.seq, TempC: t.temp, Setpoint: t.setpoint})
	return b
}

func (t *Thermostat) Apply(_ context.Context, raw []byte) error {
	var c Command
	if err := json.Unmarshal(raw, &c); err != nil || c.Op != "set_setpoint" {
		return errors.New("unsupported command")
	}
	t.setpoint = c.Value // a real device would drive its actuator here
	return nil
}
