package fh

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
)

func TestH2RaceWindowUpdateResetAndDrain(t *testing.T) {
	h := newTestH2Conn(t)
	ctx, cancel := context.WithCancel(context.Background())
	h.mu.Lock()
	h.lastStream = 1
	h.streams[1] = &h2Stream{
		id:         1,
		sendWindow: h2InitialWindow,
		recvWindow: h2InitialWindow,
		ctx:        ctx,
		cancel:     cancel,
	}
	h.mu.Unlock()

	var inc [4]byte
	binary.BigEndian.PutUint32(inc[:], 1)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = h.handleWindowUpdate(h2Frame{typ: h2WindowUpdate, streamID: 1, payload: inc[:]})
			}
		}()
	}
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.resetStream(1)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.startDrain()
		}()
	}
	wg.Wait()
}

func TestH2RaceConcurrentFrameHandling(t *testing.T) {
	h := newTestH2Conn(t)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = h.handleFrame(h2Frame{typ: 99, streamID: uint32(i)})
			_ = h.handleFrame(h2Frame{typ: h2GoAway, streamID: 0, payload: make([]byte, 8)})
		}(i)
	}
	wg.Wait()
}
