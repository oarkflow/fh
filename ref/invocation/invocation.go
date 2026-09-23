package invocation

import (
	"bytes"
	"maps"
	"slices"
	"time"
)

// ID uniquely identifies one invocation.
type ID string

// IntentID identifies which intent is being invoked.
type IntentID string

// Invocation is the transport-neutral input to the execution kernel.
// It is deeply immutable — all byte slices and maps are owned copies.
type Invocation struct {
	ID        ID
	Intent    IntentID
	Input     Input
	Principal PrincipalHint
	Metadata  Metadata
	Transport Transport
	Received  time.Time
}

// Input holds the raw invocation payload. Immutable after creation.
type Input struct {
	raw         []byte // owned copy — never exposes backing array
	contentType string
}

// NewInput creates an Input with an owned, cloned copy of raw bytes.
func NewInput(raw []byte, contentType string) Input {
	return Input{
		raw:         bytes.Clone(raw), // actually immutable
		contentType: contentType,
	}
}

// NewInputDirect creates an Input without cloning the byte slice.
// Callers must not mutate raw while the invocation is in flight.
func NewInputDirect(raw []byte, contentType string) Input {
	return Input{
		raw:         raw,
		contentType: contentType,
	}
}

// Bytes returns a copy of the raw input. Callers cannot mutate the original.
func (i Input) Bytes() []byte       { return bytes.Clone(i.raw) }
// RawBytes returns the internal byte slice without cloning. For read-only operations.
func (i Input) RawBytes() []byte    { return i.raw }
func (i Input) ContentType() string { return i.contentType }
func (i Input) Len() int            { return len(i.raw) }

// PrincipalHint carries unauthenticated identity hints from the transport.
// The auth capability must verify these before they become facts.
type PrincipalHint struct {
	BearerToken string
	APIKey      string
	ClientCert  []byte
	SessionID   string
}

// NewPrincipalHint creates a PrincipalHint ensuring client cert is cloned.
func NewPrincipalHint(bearer, apiKey string, cert []byte, sessionID string) PrincipalHint {
	return PrincipalHint{
		BearerToken: bearer,
		APIKey:      apiKey,
		ClientCert:  bytes.Clone(cert),
		SessionID:   sessionID,
	}
}

// Metadata is transport-specific context. The core kernel never inspects it;
// only transport-aware capabilities read their own metadata type.
type Metadata interface {
	TransportName() string
}

// Transport identifies the inbound transport.
type Transport struct {
	Protocol string // "http", "grpc", "ws", "queue", "cli"
	TLS      bool
	RemoteIP string
}

// ── Transport-specific metadata (not in core path) ─────────────────────

// HTTPMeta carries HTTP-specific request information.
type HTTPMeta struct {
	Method  string
	Path    string
	Route   string
	Host    string
	Headers map[string][]string // deep copy at creation
	Query   map[string][]string // deep copy at creation
	Params  map[string]string   // deep copy at creation
}

func (HTTPMeta) TransportName() string { return "http" }

// NewHTTPMeta creates a deeply cloned HTTPMeta.
func NewHTTPMeta(method, path, route, host string, headers, query map[string][]string, params map[string]string) HTTPMeta {
	clonedHeaders := make(map[string][]string, len(headers))
	for k, v := range headers {
		clonedHeaders[k] = slices.Clone(v)
	}

	clonedQuery := make(map[string][]string, len(query))
	for k, v := range query {
		clonedQuery[k] = slices.Clone(v)
	}

	return HTTPMeta{
		Method:  method,
		Path:    path,
		Route:   route,
		Host:    host,
		Headers: clonedHeaders,
		Query:   clonedQuery,
		Params:  maps.Clone(params),
	}
}

// NewHTTPMetaDirect creates an HTTPMeta taking ownership of maps without re-cloning.
func NewHTTPMetaDirect(method, path, route, host string, headers, query map[string][]string, params map[string]string) HTTPMeta {
	return HTTPMeta{
		Method:  method,
		Path:    path,
		Route:   route,
		Host:    host,
		Headers: headers,
		Query:   query,
		Params:  params,
	}
}

// GRPCMeta carries gRPC-specific request information.
type GRPCMeta struct {
	Service  string
	Method   string
	Metadata map[string][]string
}

func (GRPCMeta) TransportName() string { return "grpc" }

// NewGRPCMeta creates a deeply cloned GRPCMeta.
func NewGRPCMeta(service, method string, md map[string][]string) GRPCMeta {
	clonedMD := make(map[string][]string, len(md))
	for k, v := range md {
		clonedMD[k] = slices.Clone(v)
	}
	return GRPCMeta{
		Service:  service,
		Method:   method,
		Metadata: clonedMD,
	}
}

// QueueMeta carries queue message metadata.
type QueueMeta struct {
	Topic     string
	Partition int
	MessageID string
	Headers   map[string]string
}

func (QueueMeta) TransportName() string { return "queue" }

// NewQueueMeta creates a deeply cloned QueueMeta.
func NewQueueMeta(topic string, partition int, messageID string, headers map[string]string) QueueMeta {
	return QueueMeta{
		Topic:     topic,
		Partition: partition,
		MessageID: messageID,
		Headers:   maps.Clone(headers),
	}
}

// WSMeta carries WebSocket frame metadata.
type WSMeta struct {
	ConnectionID string
	MessageType  string
}

func (WSMeta) TransportName() string { return "ws" }

// CLIMeta carries CLI invocation metadata.
type CLIMeta struct {
	Args    []string
	Env     map[string]string
	WorkDir string
}

func (CLIMeta) TransportName() string { return "cli" }

// NewCLIMeta creates a deeply cloned CLIMeta.
func NewCLIMeta(args []string, env map[string]string, workDir string) CLIMeta {
	return CLIMeta{
		Args:    slices.Clone(args),
		Env:     maps.Clone(env),
		WorkDir: workDir,
	}
}
