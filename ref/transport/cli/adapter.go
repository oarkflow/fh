package cli

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// Command returns a CLI execution handler for an intent.
func Command(engine *runtime.Engine, intentName string) func(ctx context.Context, args []string, stdin []byte) ([]byte, error) {
	return func(ctx context.Context, args []string, stdin []byte) ([]byte, error) {
		inv := &invocation.Invocation{
			Intent:    invocation.IntentID(intentName),
			Input:     invocation.NewInput(stdin, "application/json"),
			Metadata:  invocation.NewCLIMeta(args, nil, ""),
			Transport: invocation.Transport{Protocol: "cli"},
		}

		result, err := engine.Dispatch(ctx, inv)
		if err != nil {
			return nil, fmt.Errorf("command execution error: %w", err)
		}

		out, err := json.MarshalIndent(result.Value, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("failed to encode output: %w", err)
		}

		return out, nil
	}
}
