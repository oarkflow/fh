// Put this in your app, not in the websocket package.
// It assumes your websocket package is importable as github.com/oarkflow/fh/pkg/websocket.
package main

import (
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/pkg/websocket"
)

func main() {
	hub := websocket.NewEventHub(websocket.EventHubConfig{
		Auth: func(c *websocket.EventConn, env websocket.Envelope) error {
			// In production validate JWT/session in metadata extraction below.
			// Do not trust query tokens unless they are short-lived and verified.
			if c == nil || c.Meta["user_id"] == "" {
				return websocket.ErrEventUnauthorized
			}
			return nil
		},
		Authorize: func(c *websocket.EventConn, action, topic, channel string) error {
			// Example tenant isolation. Only allow topics under user's tenant.
			if c == nil {
				return websocket.ErrEventForbidden
			}
			tenant := c.Meta["tenant"]
			if topic != "" && topic != "tenant:"+tenant {
				return websocket.ErrEventForbidden
			}
			return nil
		},
		OnError: func(c *websocket.EventConn, err error) {
			if c != nil {
				log.Println("ws event error", c.ID, err)
				return
			}
			log.Println("ws event error", err)
		},
		OnConnect: func(c *websocket.EventConn) {
			log.Println("ws connected", c.ID, c.Meta)
		},
		OnDisconnect: func(c *websocket.EventConn, err error) {
			log.Println("ws disconnected", c.ID, err)
		},
	})

	hub.Use(websocket.RecoverMiddleware(func(v any) { log.Println("panic", v) }))

	hub.On("chat.message", func(ctx *websocket.Context) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := ctx.Bind(&in); err != nil {
			return nil, err
		}
		if strings.TrimSpace(in.Text) == "" {
			return nil, errors.New("empty message")
		}

		msg := map[string]any{
			"from": ctx.Conn.Meta["user_id"],
			"text": in.Text,
			"ts":   ctx.Hub.Stats().IncomingEnvelopes,
		}
		if err := ctx.Hub.BroadcastEvent(ctx.Envelope.Topic, ctx.Envelope.Channel, "chat.message", msg); err != nil {
			return nil, err
		}
		return map[string]any{"stored": true}, nil
	})

	hub.On("notify.me", func(ctx *websocket.Context) (any, error) {
		return map[string]any{"message": "private notification", "clientId": ctx.Conn.ID}, nil
	})

	wsConfig := websocket.DefaultConfig()
	wsConfig.AllowedOrigins = []string{"http://localhost:3000", "https://app.example.com"}
	wsConfig.Subprotocols = []string{"eventws.v1"}
	wsConfig.MaxMessageSize = 1 << 20
	wsConfig.MaxMessagesPerSecond = 64

	app := fh.New()
	app.Static("/", "./views", fh.StaticConfig{Index: "index.html"})
	// Recommended: use the combined handler so metadata can be extracted from *fh.Ctx safely.
	app.Get("/ws", hub.Handler(wsConfig, func(c *fh.Ctx) map[string]string {
		// Replace this with real JWT/session validation.
		// Example: Authorization: Bearer <token>, secure cookie, or short-lived signed query token.
		return map[string]string{"user_id": "u_123", "tenant": "acme"}
	}))

	// Alternative manual style, now supported because EventHub has Add and Context aliases:
	// app.Get("/ws-manual", websocket.NewWithConfig(wsConfig, func(conn *websocket.Conn) error {
	// 	ec := hub.Add(conn, map[string]string{"user_id": "u_123", "tenant": "acme"})
	// 	if ec == nil { return websocket.ErrEventClosed }
	// 	_ = ec.Join("tenant:acme", "chat")
	// 	return hub.Serve(ec)
	// }))

	app.Get("/stats", func(c *fh.Ctx) error {
		c.Set("Content-Type", "application/json")
		b, _ := json.MarshalIndent(hub.Stats(), "", "  ")
		return c.Send(b)
	})

	log.Println("listening on :3000")
	log.Fatal(app.Listen(":3000"))
}
