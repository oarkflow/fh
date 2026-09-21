package grpc

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// Standard gRPC status codes.
const (
	CodeOK                 uint32 = 0
	CodeCanceled           uint32 = 1
	CodeUnknown            uint32 = 2
	CodeInvalidArgument    uint32 = 3
	CodeDeadlineExceeded   uint32 = 4
	CodeNotFound           uint32 = 5
	CodeAlreadyExists      uint32 = 6
	CodePermissionDenied   uint32 = 7
	CodeResourceExhausted  uint32 = 8
	CodeFailedPrecondition uint32 = 9
	CodeAborted            uint32 = 10
	CodeOutOfRange         uint32 = 11
	CodeUnimplemented      uint32 = 12
	CodeInternal           uint32 = 13
	CodeUnavailable        uint32 = 14
	CodeDataLoss           uint32 = 15
	CodeUnauthenticated    uint32 = 16
)

// CategoryToGRPCCode maps intent.Category to standard gRPC status code.
func CategoryToGRPCCode(cat intent.Category) uint32 {
	switch cat {
	case intent.CategoryInvalidInput:
		return CodeInvalidArgument
	case intent.CategoryNotFound:
		return CodeNotFound
	case intent.CategoryConflict:
		return CodeAlreadyExists
	case intent.CategoryPermission:
		return CodePermissionDenied
	case intent.CategoryAuth:
		return CodeUnauthenticated
	case intent.CategoryRateLimit:
		return CodeResourceExhausted
	case intent.CategoryUnavailable:
		return CodeUnavailable
	case intent.CategoryTimeout:
		return CodeDeadlineExceeded
	default:
		return CodeInternal
	}
}

// UnaryHandler dispatches a gRPC unary call as an intent.
func UnaryHandler(engine *runtime.Engine, intentName intent.Name) func(ctx context.Context, req []byte, md map[string][]string) ([]byte, error) {
	return func(ctx context.Context, req []byte, md map[string][]string) ([]byte, error) {
		token := ""
		if authList, ok := md["authorization"]; ok && len(authList) > 0 {
			auth := authList[0]
			if len(auth) > 7 && auth[:7] == "Bearer " {
				token = auth[7:]
			}
		}

		inv := &invocation.Invocation{
			Intent: invocation.IntentID(intentName),
			Input:  invocation.NewInput(req, "application/grpc+proto"),
			Principal: invocation.PrincipalHint{
				BearerToken: token,
			},
			Metadata: invocation.NewGRPCMeta(string(intentName), "Unary", md),
			Transport: invocation.Transport{
				Protocol: "grpc",
			},
		}

		result, err := engine.Dispatch(ctx, inv)
		if err != nil {
			if f, ok := err.(intent.Failure); ok {
				return nil, fmt.Errorf("rpc error: code = %d desc = %s", CategoryToGRPCCode(f.Category), f.Message)
			}
			return nil, err
		}

		return json.Marshal(result.Value)
	}
}
