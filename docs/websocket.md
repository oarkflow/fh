# WebSocket

fh provides an RFC 6455 implementation in `pkg/websocket` and supports both
HTTP/1.1 Upgrade and HTTP/2 extended CONNECT (RFC 8441). Application code uses
the same handler on either transport.

## Low-level handler

```go
package main

import (
    "log"

    "github.com/oarkflow/fh"
    "github.com/oarkflow/fh/pkg/websocket"
)

func main() {
    app := fh.New(fh.WithSecureByDefault(true))

    cfg := websocket.DefaultConfig()
    cfg.AllowedOrigins = []string{"https://app.example.com"}

    app.Get("/ws", websocket.NewWithConfig(cfg, func(conn *websocket.Conn) error {
        for {
            opcode, payload, err := conn.ReadMessage()
            if err != nil {
                if websocket.IsNormalClose(err) {
                    return nil
                }
                return err
            }
            if opcode == websocket.Text || opcode == websocket.Binary {
                if err := conn.WriteMessage(opcode, payload); err != nil {
                    return err
                }
            }
        }
    }))

    log.Fatal(app.ListenWithGracefulShutdown(":8080"))
}
```

`websocket.New(handler)` uses bounded defaults. `NewWithConfig` additionally
configures message/frame/fragment limits, read and write deadlines, heartbeat,
message rate, origins, subprotocols, connection management and callbacks.

Browser requests containing `Origin` are rejected when neither
`AllowedOrigins` nor `CheckOrigin` is configured. Do not set `AllowAllOrigins`
for a public browser endpoint.

## Configuration

```go
type Config struct {
    MaxMessageSize       int           // default 1 MiB
    MaxFrameSize         int           // default 64 KiB
    MaxFragments         int           // default 32
    ReadTimeout          time.Duration // default 90s
    WriteTimeout         time.Duration // default 10s
    PingInterval         time.Duration // default 30s
    PongTimeout          time.Duration // default 75s
    MaxMessagesPerSecond int           // default 128
    AllowAllOrigins      bool
    AllowedOrigins       []string
    CheckOrigin          func(fh.Ctx) bool
    Subprotocols         []string
    EnableHeartbeat      bool
    Manager              *Manager
    OnOpen               func(*Conn)
    OnClose              func(*Conn, error)
    OnError              func(*Conn, error)
    OnMessage            func(*Conn, byte, int64)
}
```

Opcodes are `websocket.Continuation`, `Text`, `Binary`, `Close`, `Ping` and
`Pong`. Important connection methods include `ReadMessage`, `WriteMessage`,
`ReadJSON`, `WriteJSON`, `Ping`, `Pong`, `CloseWithStatus`, deadline
setters and `SetReadLimit`.

## EventHub

`EventHub` adds typed JSON envelopes, event handlers, topic/channel
subscriptions, request/ack correlation, bounded writer queues, authorization,
presence and metrics.

```go
hub := websocket.NewEventHub(websocket.EventHubConfig{
    Auth: func(client *websocket.EventConn, env websocket.Envelope) error {
        // Authenticate every non-ack envelope. A successful upgrade alone is
        // not permanent authorization.
        return nil
    },
    Authorize: func(client *websocket.EventConn, action, topic, channel string) error {
        // Enforce subscribe, unsubscribe and fanout policy.
        return nil
    },
})
defer hub.Close()

hub.On("chat.message", func(ctx *websocket.HandlerContext) (any, error) {
    var input struct {
        Text string `json:"text"`
    }
    if err := ctx.Bind(&input); err != nil {
        return nil, err
    }
    return map[string]any{"received": input.Text}, nil
})

wsCfg := websocket.DefaultConfig()
wsCfg.AllowedOrigins = []string{"https://app.example.com"}

app.Get("/ws", hub.Handler(wsCfg, func(c fh.Ctx) map[string]string {
    // Return only server-validated metadata.
    return map[string]string{"remote_ip": c.IP()}
}))

_ = hub.BroadcastEvent("chat", "general", "chat.message", map[string]any{
    "text": "maintenance starts soon",
})
```

The client envelope protocol supports emit, subscribe, unsubscribe, ack,
request, broadcast, notify, presence, hello and error messages. Incoming
envelopes and payloads are independently bounded by `MaxEnvelopeBytes` and
`MaxPayloadBytes`.

Key server APIs:

```go
hub.Use(middleware)
hub.On(event, handler, middleware...)
hub.OnAny(handler)
hub.Off(event)
hub.EmitTo(clientID, event, payload)
hub.RequestTo(clientID, event, payload, timeout)
hub.BroadcastEvent(topic, channel, event, payload)
hub.NotifyEvent(topic, channel, event, payload)
hub.Client(clientID)
hub.Clients()
hub.Subscriptions()
hub.Stats()
hub.Close()
```

`EventConn` exposes `Emit`, `EmitScoped`, `Request`, `RequestScoped`, `Ack`,
`Join`, `Leave`, `Subscriptions`, metadata access and `Close`.

## Production rules

- Authenticate the HTTP upgrade and reauthorize every event/topic operation.
- Use exact allowed origins for browser clients.
- Keep message, frame, fragment, rate, queue and connection limits enabled.
- Apply idle deadlines and heartbeat checks.
- Treat metadata extracted from headers as untrusted unless prior middleware
  validated the proxy or authentication chain.
- Close the hub during application shutdown.
- Use a shared authorization source when multiple instances serve the same
  logical room; EventHub membership itself is process-local.

See [HTTP/2](http2.md#extended-connect-rfc-8441) for transport details and
[Security](security.md#browser-and-websocket-security) for deployment guidance.
