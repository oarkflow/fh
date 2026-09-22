package platform

import (
	"bytes"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/oarkflow/fh/ref/platform/spi"
)

// Storage and file actions.
//
// Content crosses the BCL boundary as base64 for binary and as text otherwise,
// because a fact has to be JSON-serialisable to travel through a queue or be
// persisted on a process run. The encoding is explicit in the config rather than
// guessed: a node that says `encoding "base64"` is unambiguous, and a node that
// guessed would eventually guess wrong about a UTF-8 PDF.

func registerStorageActions(r *Registry) {
	mustAction(r, "storage.put", storagePutAction, ActionInfo{
		Family:       "storage",
		Summary:      "Store an object and publish its key, size and checksum",
		ResourceKind: "storage",
		Provides:     "An object with key, size, etag and content_type",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "template", Required: true},
			{Name: "content_fact", Type: "fact", Required: true},
			{Name: "encoding", Type: "string", Default: "text", Summary: "text, base64 or json"},
			{Name: "content_type", Type: "template", Default: "application/octet-stream"},
		},
	})

	mustAction(r, "storage.get", storageGetAction, ActionInfo{
		Family:       "storage",
		Summary:      "Read an object",
		ResourceKind: "storage",
		Provides:     "An object with content and metadata",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "key", Type: "template", Required: true},
			{Name: "encoding", Type: "string", Default: "text", Summary: "text, base64 or json"},
			{Name: "optional", Type: "bool", Summary: "Publish null instead of failing when the object is absent"},
			{Name: "max_bytes", Type: "int", Default: "10485760"},
		},
	})

	mustAction(r, "storage.delete", storageDeleteAction, ActionInfo{
		Family:       "storage",
		Summary:      "Remove an object",
		ResourceKind: "storage",
		Kind:         "effect",
		Config:       []ConfigField{{Name: "key", Type: "template", Required: true}},
	})

	mustAction(r, "storage.list", storageListAction, ActionInfo{
		Family:       "storage",
		Summary:      "List objects under a prefix",
		ResourceKind: "storage",
		Provides:     "A list of object metadata",
		Kind:         "read",
		Config: []ConfigField{
			{Name: "prefix", Type: "template"},
			{Name: "limit", Type: "int", Default: "100"},
		},
	})

	mustAction(r, "storage.presign", storagePresignAction, ActionInfo{
		Family:       "storage",
		Summary:      "Mint a time-limited URL. Needs a provider that can sign one.",
		ResourceKind: "storage",
		Provides:     "An object with url and expires_at",
		Kind:         "effect",
		Config: []ConfigField{
			{Name: "key", Type: "template", Required: true},
			{Name: "method", Type: "string", Default: "GET"},
			{Name: "ttl", Type: "duration", Default: "15m"},
		},
	})

	mustAction(r, "file.csv_encode", csvEncodeAction, ActionInfo{
		Family:   "storage",
		Summary:  "Render a list of records as CSV text",
		Provides: "The CSV document",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "columns", Type: "[]string", Summary: "Column order. Empty uses the first record's keys, sorted."},
			{Name: "header", Type: "bool", Default: "true"},
		},
	})

	mustAction(r, "file.csv_decode", csvDecodeAction, ActionInfo{
		Family:   "storage",
		Summary:  "Parse CSV text into a list of records",
		Provides: "A list of record objects",
		Kind:     "pure",
		Config: []ConfigField{
			{Name: "source_fact", Type: "fact", Required: true},
			{Name: "header", Type: "bool", Default: "true", Summary: "Treat the first row as column names"},
			{Name: "max_rows", Type: "int", Default: "10000"},
		},
	})
}

// storageKey compiles the templated key every storage action takes.
type storageCall struct {
	store    spi.ObjectStore
	key      *Template
	encoding string
}

func compileStorageCall(build BuildContext, spec NodeSpec) (*storageCall, error) {
	store, err := requireResource[spi.ObjectStore](build, spec, "a storage resource")
	if err != nil {
		return nil, err
	}
	key, err := configTemplate(spec.Config, "key", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	if key == nil {
		return nil, fmt.Errorf("node %q: config.key is required", spec.Name)
	}
	encoding := strings.ToLower(configString(spec.Config, "encoding", "text"))
	switch encoding {
	case "text", "base64", "json":
	default:
		return nil, fmt.Errorf("node %q: encoding must be text, base64 or json", spec.Name)
	}
	return &storageCall{store: store, key: key, encoding: encoding}, nil
}

var storagePutAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileStorageCall(build, spec)
	if err != nil {
		return nil, err
	}
	contentFact, err := requiredString(spec.Config, "content_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	contentType, err := configTemplate(spec.Config, "content_type", "application/octet-stream")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		env := actionEnv(ctx)
		key, err := call.key.Render(env)
		if err != nil {
			return ActionResult{}, err
		}
		value, found := resolvePath(ctx.Inputs, contentFact)
		if !found {
			return ActionResult{}, invalidInput("no content at %q", contentFact)
		}
		content, err := encodeContent(call.encoding, value)
		if err != nil {
			return ActionResult{}, err
		}
		mime, err := contentType.Render(env)
		if err != nil {
			return ActionResult{}, err
		}
		info, err := call.store.Put(ctx.Context, key, bytes.NewReader(content), mime)
		if err != nil {
			return ActionResult{}, storageFailure(err)
		}
		return acknowledgement(spec, map[string]any{
			"key":          info.Key,
			"size":         info.Size,
			"etag":         info.ETag,
			"content_type": info.ContentType,
		}), nil
	}), nil
})

var storageGetAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileStorageCall(build, spec)
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	optional := configBool(spec.Config, "optional", false)
	maxBytes, err := configInt64(spec.Config, "max_bytes", 10<<20)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := call.key.Render(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		reader, info, err := call.store.Get(ctx.Context, key)
		if err != nil {
			if optional && isNotFound(err) {
				return singleOutput(spec, nil), nil
			}
			return ActionResult{}, storageFailure(err)
		}
		defer reader.Close()

		// Bound the read even though the store reported a size: a store's metadata
		// and its body can disagree, and this node's job is to publish a fact into
		// memory, not to stream.
		content, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
		if err != nil {
			return ActionResult{}, storageFailure(err)
		}
		if int64(len(content)) > maxBytes {
			return ActionResult{}, invalidInput("object %q is larger than this node's max_bytes of %d", key, maxBytes)
		}
		decoded, err := decodeContent(call.encoding, content)
		if err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, map[string]any{
			"key":          info.Key,
			"size":         info.Size,
			"etag":         info.ETag,
			"content_type": info.ContentType,
			"content":      decoded,
		}), nil
	}), nil
})

var storageDeleteAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	call, err := compileStorageCall(build, spec)
	if err != nil {
		return nil, err
	}
	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		key, err := call.key.Render(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		if err := call.store.Delete(ctx.Context, key); err != nil {
			return ActionResult{}, storageFailure(err)
		}
		return acknowledgement(spec, true), nil
	}), nil
})

var storageListAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	store, err := requireResource[spi.ObjectStore](build, spec, "a storage resource")
	if err != nil {
		return nil, err
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	prefix, err := configTemplate(spec.Config, "prefix", "")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	limit, err := configInt(spec.Config, "limit", 100)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved := ""
		if prefix != nil {
			if resolved, err = prefix.Render(actionEnv(ctx)); err != nil {
				return ActionResult{}, err
			}
		}
		objects, err := store.List(ctx.Context, resolved, limit)
		if err != nil {
			return ActionResult{}, storageFailure(err)
		}
		return singleOutput(spec, objects), nil
	}), nil
})

var storagePresignAction = ActionFactoryFunc(func(build BuildContext, spec NodeSpec) (Action, error) {
	store, err := requireResource[spi.ObjectStore](build, spec, "a storage resource")
	if err != nil {
		return nil, err
	}
	presigner, ok := store.(spi.ObjectPresigner)
	if !ok {
		return nil, fmt.Errorf("node %q: storage resource %q cannot mint pre-signed URLs. Serve the object through a route instead, or register an adapter that can sign",
			spec.Name, spec.Resource)
	}
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	key, err := configTemplate(spec.Config, "key", "")
	if err != nil || key == nil {
		return nil, fmt.Errorf("node %q: config.key is required", spec.Name)
	}
	method := strings.ToUpper(configString(spec.Config, "method", "GET"))
	ttl, err := configDuration(spec.Config, "ttl", 15*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		resolved, err := key.Render(actionEnv(ctx))
		if err != nil {
			return ActionResult{}, err
		}
		url, err := presigner.Presign(ctx.Context, resolved, method, ttl)
		if err != nil {
			return ActionResult{}, storageFailure(err)
		}
		return singleOutput(spec, map[string]any{"url": url, "expires_at": ctx.Now.Add(ttl)}), nil
	}), nil
})

// ---------------------------------------------------------------------------
// CSV
// ---------------------------------------------------------------------------

var csvEncodeAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	columns := configStrings(spec.Config, "columns")
	header := configBool(spec.Config, "header", true)

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		value, _ := resolvePath(ctx.Inputs, sourceFact)
		records, err := requiredList(value, sourceFact)
		if err != nil {
			return ActionResult{}, err
		}
		fields := columns
		if len(fields) == 0 && len(records) > 0 {
			if first, ok := records[0].(map[string]any); ok {
				for key := range first {
					fields = append(fields, key)
				}
				// Sorted, so the same data always produces the same file — which
				// matters the moment anything downstream diffs or checksums it.
				sortStrings(fields)
			}
		}
		var out bytes.Buffer
		writer := csv.NewWriter(&out)
		if header && len(fields) > 0 {
			if err := writer.Write(fields); err != nil {
				return ActionResult{}, err
			}
		}
		for _, record := range records {
			object, ok := record.(map[string]any)
			if !ok {
				return ActionResult{}, invalidInput("every CSV record must be an object")
			}
			row := make([]string, len(fields))
			for i, field := range fields {
				row[i] = Stringify(object[field])
			}
			if err := writer.Write(row); err != nil {
				return ActionResult{}, err
			}
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return ActionResult{}, err
		}
		return singleOutput(spec, out.String()), nil
	}), nil
})

var csvDecodeAction = ActionFactoryFunc(func(_ BuildContext, spec NodeSpec) (Action, error) {
	if err := exactlyOneOutput(spec); err != nil {
		return nil, err
	}
	sourceFact, err := requiredString(spec.Config, "source_fact")
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}
	header := configBool(spec.Config, "header", true)
	maxRows, err := configInt(spec.Config, "max_rows", 10000)
	if err != nil {
		return nil, fmt.Errorf("node %q: %w", spec.Name, err)
	}

	return ActionFunc(func(ctx *ActionContext) (ActionResult, error) {
		raw, _ := resolvePath(ctx.Inputs, sourceFact)
		reader := csv.NewReader(strings.NewReader(Stringify(raw)))
		reader.FieldsPerRecord = -1
		rows, err := reader.ReadAll()
		if err != nil {
			return ActionResult{}, invalidInput("the CSV could not be parsed: %v", err)
		}
		if len(rows) == 0 {
			return singleOutput(spec, []any{}), nil
		}
		var fields []string
		start := 0
		if header {
			fields, start = rows[0], 1
		} else {
			fields = make([]string, len(rows[0]))
			for i := range fields {
				fields[i] = fmt.Sprintf("column_%d", i+1)
			}
		}
		if len(rows)-start > maxRows {
			return ActionResult{}, invalidInput("the CSV has %d rows, over this node's max_rows of %d", len(rows)-start, maxRows)
		}
		out := make([]any, 0, len(rows)-start)
		for _, row := range rows[start:] {
			record := make(map[string]any, len(fields))
			for i, field := range fields {
				if i < len(row) {
					record[field] = row[i]
					continue
				}
				record[field] = ""
			}
			out = append(out, record)
		}
		return singleOutput(spec, out), nil
	}), nil
})

// ---------------------------------------------------------------------------
// Content encoding
// ---------------------------------------------------------------------------

func encodeContent(encoding string, value any) ([]byte, error) {
	switch encoding {
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(Stringify(value))
		if err != nil {
			return nil, invalidInput("the content is not valid base64: %v", err)
		}
		return decoded, nil
	case "json":
		encoded, err := json.Marshal(value)
		if err != nil {
			return nil, invalidInput("the content cannot be serialised: %v", err)
		}
		return encoded, nil
	default:
		return []byte(Stringify(value)), nil
	}
}

func decodeContent(encoding string, content []byte) (any, error) {
	switch encoding {
	case "base64":
		return base64.StdEncoding.EncodeToString(content), nil
	case "json":
		var decoded any
		if err := json.Unmarshal(content, &decoded); err != nil {
			return nil, invalidInput("the stored object is not valid JSON: %v", err)
		}
		return decoded, nil
	default:
		return string(content), nil
	}
}

// storageFailure keeps a not-found from being reported as an internal error,
// which is what an unmapped store error would otherwise become.
func storageFailure(err error) error {
	if isNotFound(err) {
		return err
	}
	if err == nil {
		return nil
	}
	return unavailable("the storage operation failed: %v", err)
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
