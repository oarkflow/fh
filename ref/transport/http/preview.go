package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/oarkflow/fh/ref/intent"
	"github.com/oarkflow/fh/ref/invocation"
	"github.com/oarkflow/fh/ref/runtime"
)

// EnginePreview adapts a trusted REF read intent to rollout shadow mode.
// The engine refuses explicit effect nodes and suppresses returned effect plans.
func EnginePreview(engine *runtime.Engine, name intent.Name, routePattern string) PreviewFunc {
	return func(ctx context.Context, req RequestSnapshot) (ResponseSnapshot, error) {
		u, err := url.Parse(req.URL)
		if err != nil {
			return ResponseSnapshot{}, err
		}
		query := SnapshotQuery(req)
		params := make(map[string]string)
		patternParts := strings.Split(strings.Trim(routePattern, "/"), "/")
		pathParts := strings.Split(strings.Trim(u.Path, "/"), "/")
		if len(patternParts) == len(pathParts) {
			for i, part := range patternParts {
				if strings.HasPrefix(part, ":") {
					params[strings.TrimPrefix(part, ":")] = pathParts[i]
				}
			}
		}
		headers := make(map[string][]string, len(req.Header))
		for k, v := range req.Header {
			headers[k] = append([]string(nil), v...)
		}
		inv := &invocation.Invocation{
			ID: invocation.ID(req.RequestID), Intent: invocation.IntentID(name),
			Input:     invocation.NewInput(req.Body, req.Header.Get("Content-Type")),
			Principal: invocation.NewPrincipalHint(previewBearer(req.Header.Get("Authorization")), req.Header.Get("X-API-Key"), nil, ""),
			Metadata:  invocation.NewHTTPMeta(req.Method, u.Path, routePattern, req.Host, headers, query, params),
			Transport: invocation.Transport{Protocol: "http", RemoteIP: req.RemoteAddr, TLS: req.TLS},
		}
		result, err := engine.DispatchPreview(ctx, inv)
		if err != nil {
			return ResponseSnapshot{}, err
		}
		body, err := json.Marshal(result.Value)
		if err != nil {
			return ResponseSnapshot{}, fmt.Errorf("preview response encoding failed")
		}
		return ResponseSnapshot{Status: 200, Header: map[string][]string{"Content-Type": {"application/json"}}, Body: body}, nil
	}
}

func previewBearer(value string) string {
	if len(value) > 7 && strings.EqualFold(value[:7], "Bearer ") {
		return strings.TrimSpace(value[7:])
	}
	return ""
}
