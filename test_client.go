package fh

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

type pipeListener struct {
	conn net.Conn
	done chan struct{}
	once sync.Once
}

func newPipeListener(c net.Conn) *pipeListener {
	return &pipeListener{conn: c, done: make(chan struct{})}
}

func (l *pipeListener) Accept() (net.Conn, error) {
	if l.conn != nil {
		c := l.conn
		l.conn = nil
		return c, nil
	}
	<-l.done
	return nil, io.EOF
}

func (l *pipeListener) Close() error { l.once.Do(func() { close(l.done) }); return nil }
func (*pipeListener) Addr() net.Addr { return &net.IPAddr{} }

// Test executes a standard *http.Request directly against app in-memory over net.Pipe() without binding TCP ports.
func (a *App) Test(req *http.Request, msTimeout ...int) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("fh: nil request passed to app.Test")
	}

	timeoutDur := 10 * time.Second
	if len(msTimeout) > 0 && msTimeout[0] > 0 {
		timeoutDur = time.Duration(msTimeout[0]) * time.Millisecond
	}

	client, server := net.Pipe()
	ln := newPipeListener(server)

	go func() {
		_ = a.Serve(ln)
	}()

	errCh := make(chan error, 1)
	respCh := make(chan *http.Response, 1)

	go func() {
		defer client.Close()
		if req.URL != nil && req.URL.Host == "" {
			req.URL.Host = "localhost"
		}
		if req.Header.Get("Host") == "" {
			req.Header.Set("Host", "localhost")
		}
		if req.Header.Get("Connection") == "" {
			req.Header.Set("Connection", "close")
		}

		if err := req.Write(client); err != nil {
			errCh <- fmt.Errorf("fh test: failed writing request: %w", err)
			return
		}

		reader := bufio.NewReader(client)
		resp, err := http.ReadResponse(reader, req)
		if err != nil {
			errCh <- fmt.Errorf("fh test: failed reading response: %w", err)
			return
		}
		respCh <- resp
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeoutDur)
	defer cancel()

	select {
	case err := <-errCh:
		_ = a.ShutdownWithTimeout(time.Second)
		return nil, err
	case resp := <-respCh:
		_ = a.ShutdownWithTimeout(time.Second)
		return resp, nil
	case <-ctx.Done():
		_ = client.Close()
		_ = a.ShutdownWithTimeout(time.Second)
		return nil, errors.New("fh test: request timed out")
	}
}
