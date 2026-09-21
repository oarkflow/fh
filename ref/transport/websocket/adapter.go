package websocket

import (
	"context"
	"encoding/json"

	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// Message is the standard WebSocket frame envelope for REF dispatches.
type Message struct {
	ID      string          `json:"id"`
	Intent  string          `json:"intent"`
	Payload json.RawMessage `json:"payload"`
	Token   string          `json:"token,omitempty"`
}

// Response is the structured WebSocket frame result sent back to clients.
type Response struct {
	ID      string `json:"id"`
	Success bool   `json:"success"`
	Data    any    `json:"data,omitempty"`
	Error   string `json:"error,omitempty"`
	Code    string `json:"code,omitempty"`
}

// Handler creates a WebSocket frame dispatcher for the REF engine.
func Handler(engine *runtime.Engine) func(ctx context.Context, connID string, msg Message) (Response, error) {
	return func(ctx context.Context, connID string, msg Message) (Response, error) {
		inv := &invocation.Invocation{
			ID:     invocation.ID(msg.ID),
			Intent: invocation.IntentID(msg.Intent),
			Input:  invocation.NewInput(msg.Payload, "application/json"),
			Principal: invocation.PrincipalHint{
				BearerToken: msg.Token,
			},
			Metadata: invocation.WSMeta{
				ConnectionID: connID,
				MessageType:  "text",
			},
			Transport: invocation.Transport{
				Protocol: "ws",
			},
		}

		result, err := engine.Dispatch(ctx, inv)
		if err != nil {
			resp := Response{
				ID:      msg.ID,
				Success: false,
				Error:   err.Error(),
			}
			if f, ok := err.(intent.Failure); ok {
				resp.Code = f.Code
			}
			return resp, nil
		}

		return Response{
			ID:      msg.ID,
			Success: true,
			Data:    result.Value,
		}, nil
	}
}
