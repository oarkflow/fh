package intent

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/oarkflow/fh/ref/effect"
	"github.com/oarkflow/fh/ref/execution"
	"github.com/oarkflow/fh/ref/fact"
)

// Registry stores all registered intent definitions.
type Registry struct {
	mu      sync.RWMutex
	intents map[Name]*Definition
}

// NewRegistry creates a new intent registry.
func NewRegistry() *Registry {
	return &Registry{
		intents: make(map[Name]*Definition),
	}
}

// DecoderFunc deserializes raw invocation bytes into typed input.
type DecoderFunc[I any] func(raw []byte, contentType string) (I, error)

// Register registers a strongly typed Intent[I, O] into the registry.
func Register[I, O any](r *Registry, it Intent[I, O], customDecoder ...DecoderFunc[I]) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := it.Name()
	if _, exists := r.intents[name]; exists {
		return fmt.Errorf("ref: duplicate intent %q", name)
	}

	spec := it.Spec()

	var dec DecoderFunc[I]
	if len(customDecoder) > 0 && customDecoder[0] != nil {
		dec = customDecoder[0]
	} else {
		dec = defaultDecoder[I]()
	}

	inputKey := fact.NewKey[I](string(name) + ".input")

	def := &Definition{
		Name:     name,
		Spec:     spec,
		InputKey: inputKey.Any(),
		DecodeNode: func(nc *execution.NodeContext) error {
			raw := nc.Invocation().Input
			typedInput, err := dec(raw.RawBytes(), raw.ContentType())
			if err != nil {
				return Failure{
					Code:     "INVALID_INPUT",
					Category: CategoryInvalidInput,
					Message:  fmt.Sprintf("failed to decode intent input: %v", err),
					Cause:    err,
				}
			}
			execution.Publish(nc, inputKey, typedInput)
			return nil
		},
		Run: func(nc *execution.NodeContext) (any, effect.EffectPlan, OutcomeMeta, error) {
			typedInput, err := execution.Require(nc, inputKey)
			if err != nil {
				return nil, effect.EffectPlan{}, OutcomeMeta{}, err
			}

			outcome, err := it.Run(nc, typedInput)
			if err != nil {
				return nil, effect.EffectPlan{}, OutcomeMeta{}, err
			}

			return outcome.Value, outcome.Effects, outcome.Meta, nil
		},
	}

	r.intents[name] = def
	return nil
}

// Lookup retrieves an intent definition by name.
func (r *Registry) Lookup(name Name) (*Definition, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	def, ok := r.intents[name]
	return def, ok
}

// All returns all registered intent definitions.
func (r *Registry) All() map[Name]*Definition {
	r.mu.RLock()
	defer r.mu.RUnlock()
	res := make(map[Name]*Definition, len(r.intents))
	for k, v := range r.intents {
		res[k] = v
	}
	return res
}

func defaultDecoder[I any]() DecoderFunc[I] {
	var zero I
	valType := reflect.TypeOf(zero)

	return func(raw []byte, contentType string) (I, error) {
		// If I is []byte
		if valType == reflect.TypeOf([]byte(nil)) {
			var res any = raw
			return res.(I), nil
		}
		// If I is string
		if valType == reflect.TypeOf("") {
			var res any = string(raw)
			return res.(I), nil
		}
		// If empty input or no bytes
		if len(raw) == 0 {
			return zero, nil
		}

		var target I
		if err := json.Unmarshal(raw, &target); err != nil {
			return zero, err
		}
		return target, nil
	}
}
