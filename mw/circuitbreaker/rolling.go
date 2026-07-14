package circuitbreaker

import (
	"sync"
	"time"
)

type rollingBucket struct {
	epoch    int64
	requests uint64
	failures uint64
}

type rollingWindow struct {
	mu             sync.Mutex
	generation     uint64
	bucketDuration time.Duration
	buckets        []rollingBucket
}

func newRollingWindow(window time.Duration, bucketCount int, generation uint64) *rollingWindow {
	return &rollingWindow{
		generation:     generation,
		bucketDuration: window / time.Duration(bucketCount),
		buckets:        make([]rollingBucket, bucketCount),
	}
}

func (w *rollingWindow) add(now time.Time, generation uint64, failed bool) (uint64, uint64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if generation < w.generation {
		requests, failures := w.sumLocked(now.UnixNano() / int64(w.bucketDuration))
		return requests, failures, false
	}
	if generation > w.generation {
		clear(w.buckets)
		w.generation = generation
	}

	currentEpoch := now.UnixNano() / int64(w.bucketDuration)
	index := int(currentEpoch % int64(len(w.buckets)))
	if index < 0 {
		index += len(w.buckets)
	}
	bucket := &w.buckets[index]
	if bucket.epoch != currentEpoch {
		*bucket = rollingBucket{epoch: currentEpoch}
	}
	bucket.requests++
	if failed {
		bucket.failures++
	}
	requests, failures := w.sumLocked(currentEpoch)
	return requests, failures, true
}

func (w *rollingWindow) snapshot(now time.Time) (uint64, uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	currentEpoch := now.UnixNano() / int64(w.bucketDuration)
	return w.sumLocked(currentEpoch)
}

func (w *rollingWindow) sumLocked(currentEpoch int64) (uint64, uint64) {
	oldest := currentEpoch - int64(len(w.buckets)) + 1
	var requests, failures uint64
	for i := range w.buckets {
		bucket := &w.buckets[i]
		if bucket.epoch >= oldest && bucket.epoch <= currentEpoch {
			requests += bucket.requests
			failures += bucket.failures
		}
	}
	return requests, failures
}

func (w *rollingWindow) reset(generation uint64) {
	w.mu.Lock()
	if generation >= w.generation {
		clear(w.buckets)
		w.generation = generation
	}
	w.mu.Unlock()
}
