# WebSocket

fh implements WebSocket (RFC 6455) entirely from scratch, including both low-level connection handling and a high-level pub/sub EventHub.

Every handler below works unmodified over both HTTP/1.1 (`Connection: Upgrade`)
and HTTP/2 (RFC 8441 extended CONNECT) clients — `c.Upgrade("websocket", ...)`
detects the transport automatically. See [HTTP/2 § Extended
CONNECT](http2.md#extended-connect-rfc-8441) for the protocol-level details.

## Architecture

- **`pkg/websocket`** — Core WebSocket implementation
  - **`Conn`** — Low-level WebSocket connection (read/write frames, masking, control frames, rate limiting)
  - **`EventHub`** — High-level pub/sub with rooms, topics, channels, auth, heartbeat, reconnect

## Low-Level WebSocket

### Server-Side Upgrade

```go
import "github.com/oarkflow/fh/pkg/websocket"

app.Get("/ws", func(c *fh.Ctx) error {
    return c.Upgrade("websocket", func(conn *websocket.Conn) {
        defer conn.Close()

        for {
            msgType, data, err := conn.ReadMessage()
            if err != nil {
                break
            }

            switch msgType {
            case websocket.TextMessage:
                log.Printf("Received text: %s", data)
                conn.WriteMessage(websocket.TextMessage, []byte("echo: "+string(data)))
            case websocket.BinaryMessage:
                log.Printf("Received binary: %d bytes", len(data))
                conn.WriteMessage(websocket.BinaryMessage, data)
            case websocket.PingMessage:
                conn.WriteMessage(websocket.PongMessage, nil)
            }
        }
    })
})
```

### Conn Methods

```go
conn.ReadMessage() (messageType int, data []byte, err error)
conn.WriteMessage(messageType int, data []byte) error
conn.WriteJSON(v any) error
conn.ReadJSON(v any) error
conn.Close() error
conn.SetReadLimit(limit int64)
conn.SetReadDeadline(t time.Time)
conn.SetWriteDeadline(t time.Time)
```

### Message Types

```go
websocket.TextMessage   = 1
websocket.BinaryMessage = 2
websocket.CloseMessage  = 8
websocket.PingMessage   = 9
websocket.PongMessage   = 10
```

## EventHub (High-Level Pub/Sub)

The EventHub provides a publish/subscribe pattern over WebSocket connections with rooms, topics, authentication, and acknowledgements.

### Basic Usage

```go
import "github.com/oarkflow/fh/pkg/websocket"

hub := websocket.NewEventHub()

app.Get("/ws", func(c *fh.Ctx) error {
    return c.Upgrade("websocket", func(conn *websocket.Conn) {
        client := hub.Connect(conn)
        defer hub.Disconnect(client)

        // Join rooms
        client.Join("room:general")
        client.Join("user:42")

        // Handle events
        client.On("message", func(data []byte) {
            hub.Publish("room:general", "message", data)
        })

        // Block until disconnect
        client.Wait()
    })
})
```

### Features

| Feature | Description |
|---------|-------------|
| **Rooms** | Named groups for message routing |
| **Topics** | Event types within rooms |
| **Channels** | Direct client-to-client messaging |
| **Auth** | Per-connection authentication |
| **Acknowledgements** | Reliable message delivery |
| **Heartbeat** | Keep-alive ping/pong |
| **Reconnect** | Automatic reconnection support |

### EventHub API

```go
hub := websocket.NewEventHub()

// Publish an event to all clients in a room
hub.Publish("room:general", "message", data)

// Publish to a specific client
hub.PublishTo(clientID, "private", data)

// Broadcast to all connected clients
hub.Broadcast("announcement", data)

// Get room info
clients := hub.Clients("room:general")

// Client methods
client.On("event", handler)      // register event handler
client.Once("event", handler)    // one-time handler
client.Off("event")              // remove handler
client.Emit("event", data)       // emit event to this client
client.Join("room")              // join a room
client.Leave("room")             // leave a room
client.Auth(token)               // authenticate
client.ID() string               // client ID
client.Wait()                    // block until disconnect
```

## Complete Example

See `examples/websocket/` for a complete runnable example.
