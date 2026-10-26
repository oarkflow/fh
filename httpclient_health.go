package fh

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var ErrClientNoHealthyEndpoints = errors.New("no healthy client endpoints")

// HealthSelectorConfig controls when a client endpoint is temporarily ejected.
// Zero values use a threshold of three failures and a 30-second cooldown.
type HealthSelectorConfig struct {
	FailureThreshold int
	EjectionDuration time.Duration
	// FailureStatuses marks responses that count as endpoint failures. When nil,
	// HTTP 500, 502, 503, and 504 count as failures. An empty map disables
	// status-based ejection; transport errors still count.
	FailureStatuses map[int]bool
}

// EndpointHealth is a point-in-time, concurrency-safe health snapshot.
type EndpointHealth struct {
	Endpoint      string
	Failures      int
	EjectedUntil  time.Time
	ProbeInFlight bool
}

type healthEndpoint struct {
	url          *url.URL
	failures     int
	ejectedUntil time.Time
	probe        bool
	generation   uint64
}

type healthSelection struct {
	endpoint   int
	generation uint64
	probe      bool
}

// HealthAwareSelector selects healthy endpoints in round-robin order. After
// ejection expires, only one request probes an endpoint until Report is called.
// Use it with ClientLoadBalance, which reports every completed round trip.
type HealthAwareSelector struct {
	mu        sync.Mutex
	endpoints []healthEndpoint
	inflight  map[*url.URL]healthSelection
	next      uint64
	config    HealthSelectorConfig
	now       func() time.Time
}

func NewHealthAwareSelector(config HealthSelectorConfig, raw ...string) (*HealthAwareSelector, error) {
	if config.FailureThreshold < 0 || config.EjectionDuration < 0 {
		return nil, errors.New("fh client: health selector thresholds cannot be negative")
	}
	if config.FailureThreshold == 0 {
		config.FailureThreshold = 3
	}
	if config.EjectionDuration == 0 {
		config.EjectionDuration = 30 * time.Second
	}
	if config.FailureStatuses == nil {
		config.FailureStatuses = map[int]bool{500: true, 502: true, 503: true, 504: true}
	} else {
		copyStatuses := make(map[int]bool, len(config.FailureStatuses))
		for status, failed := range config.FailureStatuses {
			copyStatuses[status] = failed
		}
		config.FailureStatuses = copyStatuses
	}
	if len(raw) == 0 {
		return nil, errors.New("fh client: no endpoints")
	}
	s := &HealthAwareSelector{
		endpoints: make([]healthEndpoint, 0, len(raw)),
		inflight:  make(map[*url.URL]healthSelection),
		config:    config,
		now:       time.Now,
	}
	seen := make(map[string]bool, len(raw))
	for _, address := range raw {
		u, err := url.Parse(address)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("fh client: invalid endpoint %q", address)
		}
		key := strings.ToLower(u.Scheme+"://"+u.Host) + u.EscapedPath()
		if seen[key] {
			return nil, fmt.Errorf("fh client: duplicate endpoint %q", address)
		}
		seen[key] = true
		s.endpoints = append(s.endpoints, healthEndpoint{url: u})
	}
	return s, nil
}

func (s *HealthAwareSelector) Next(req *http.Request) (*url.URL, error) {
	if req != nil && req.Context().Err() != nil {
		return nil, req.Context().Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	for step := 0; step < len(s.endpoints); step++ {
		index := int((s.next + uint64(step)) % uint64(len(s.endpoints)))
		endpoint := &s.endpoints[index]
		if endpoint.probe || endpoint.ejectedUntil.After(now) {
			continue
		}
		probe := !endpoint.ejectedUntil.IsZero()
		if probe {
			endpoint.probe = true
		}
		s.next = uint64(index+1) % uint64(len(s.endpoints))
		selected := *endpoint.url
		result := &selected
		s.inflight[result] = healthSelection{endpoint: index, generation: endpoint.generation, probe: probe}
		return result, nil
	}
	return nil, ErrClientNoHealthyEndpoints
}

func (s *HealthAwareSelector) Report(endpoint *url.URL, response *http.Response, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	selection, ok := s.inflight[endpoint]
	if !ok {
		return
	}
	delete(s.inflight, endpoint)
	state := &s.endpoints[selection.endpoint]
	if selection.generation != state.generation {
		return
	}
	if errors.Is(err, context.Canceled) {
		if selection.probe {
			state.probe = false
			state.ejectedUntil = s.now().Add(s.config.EjectionDuration)
			state.generation++
		}
		return
	}
	// A response is judged by its status, even if a later middleware converts
	// that status to an error (for example ClientRequireStatus on HTTP 404).
	failure := response == nil
	if response != nil {
		failure = s.config.FailureStatuses[response.StatusCode]
	}
	if failure {
		state.failures++
		if selection.probe || state.failures >= s.config.FailureThreshold {
			state.failures = s.config.FailureThreshold
			state.ejectedUntil = s.now().Add(s.config.EjectionDuration)
			state.probe = false
			state.generation++
		}
		return
	}
	state.failures = 0
	if selection.probe {
		state.ejectedUntil = time.Time{}
		state.probe = false
		state.generation++
	}
}

func (s *HealthAwareSelector) Snapshot() []EndpointHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	states := make([]EndpointHealth, len(s.endpoints))
	for i, endpoint := range s.endpoints {
		states[i] = EndpointHealth{
			Endpoint:      endpoint.url.String(),
			Failures:      endpoint.failures,
			EjectedUntil:  endpoint.ejectedUntil,
			ProbeInFlight: endpoint.probe,
		}
	}
	return states
}
