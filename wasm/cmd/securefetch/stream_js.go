//go:build js && wasm

package main

import (
	"errors"
	"io"
	"sync"
	"syscall/js"

	protocol "github.com/oarkflow/fh/pkg/securetransport"
)

// jsReadableReader copies only one bounded browser stream chunk at a time.
type jsReadableReader struct {
	reader js.Value
	chunk  []byte
	done   bool
}

func newJSReadableReader(stream js.Value) (*jsReadableReader, error) {
	if stream.IsUndefined() || stream.IsNull() {
		return nil, errors.New("fh secure fetch: response stream is missing")
	}
	reader := stream.Call("getReader")
	return &jsReadableReader{reader: reader}, nil
}

func (r *jsReadableReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.chunk) == 0 && !r.done {
		result, err := await(r.reader.Call("read"))
		if err != nil {
			return 0, err
		}
		if result.Get("done").Bool() {
			r.done = true
			break
		}
		value := result.Get("value")
		if !value.InstanceOf(js.Global().Get("Uint8Array")) {
			return 0, errors.New("fh secure fetch: stream yielded a non-byte chunk")
		}
		size := value.Get("byteLength").Int()
		if size <= 0 || size > 1<<20 {
			return 0, protocol.ErrTooLarge
		}
		r.chunk = make([]byte, size)
		if js.CopyBytesToGo(r.chunk, value) != size {
			return 0, errors.New("fh secure fetch: incomplete stream chunk")
		}
	}
	if r.done {
		return 0, io.EOF
	}
	n := copy(p, r.chunk)
	r.chunk = r.chunk[n:]
	return n, nil
}

func (r *jsReadableReader) Close() error {
	if r.done {
		return nil
	}
	r.done = true
	_, err := await(r.reader.Call("cancel"))
	return err
}

// newJSReadableStream exposes an io.Reader as a backpressured browser stream.
// A browser pull causes at most one bounded Go read.
func newJSReadableStream(reader io.Reader) js.Value {
	source := js.Global().Get("Object").New()
	var pull, cancel js.Func
	var once sync.Once
	release := func() { once.Do(func() { pull.Release(); cancel.Release() }) }
	pull = js.FuncOf(func(_ js.Value, args []js.Value) any {
		controller := args[0]
		return promise(func() (js.Value, error) {
			buffer := make([]byte, protocol.StreamChunkSize)
			n, err := reader.Read(buffer)
			if n > 0 {
				controller.Call("enqueue", uint8Array(buffer[:n]))
			}
			if err == io.EOF {
				controller.Call("close")
				release()
			} else if err != nil {
				controller.Call("error", js.Global().Get("Error").New(err.Error()))
				release()
			}
			return js.Undefined(), nil
		})
	})
	cancel = js.FuncOf(func(_ js.Value, _ []js.Value) any {
		if closer, ok := reader.(io.Closer); ok {
			_ = closer.Close()
		}
		release()
		return js.Undefined()
	})
	source.Set("pull", pull)
	source.Set("cancel", cancel)
	return js.Global().Get("ReadableStream").New(source)
}
