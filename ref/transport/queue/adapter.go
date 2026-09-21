package queue

import (
	"context"

	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// Consumer creates a queue worker message handler for an intent.
func Consumer(engine *runtime.Engine, intentName string) func(ctx context.Context, topic string, msg []byte, headers map[string]string) error {
	return func(ctx context.Context, topic string, msg []byte, headers map[string]string) error {
		msgID := ""
		token := ""
		if headers != nil {
			msgID = headers["message_id"]
			token = headers["authorization"]
		}

		inv := &invocation.Invocation{
			ID:     invocation.ID(msgID),
			Intent: invocation.IntentID(intentName),
			Input:  invocation.NewInput(msg, "application/json"),
			Principal: invocation.PrincipalHint{
				BearerToken: token,
			},
			Metadata: invocation.NewQueueMeta(topic, 0, msgID, headers),
			Transport: invocation.Transport{
				Protocol: "queue",
			},
		}

		_, err := engine.Dispatch(ctx, inv)
		return err
	}
}
