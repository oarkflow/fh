package process

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// The in-memory store.
//
// It is the right choice for tests and for a single-process deployment that does
// not need durability, and the wrong choice for anything else — which it says
// plainly rather than being presented as an equal option. It implements the same
// contract as the SQL store, including the optimistic revisions and the atomic
// lease, so a test exercising a concurrency path here is exercising the same logic
// the SQL store enforces.
//
// Everything is deep-copied on the way in and out. Without that, a caller
// mutating a returned run would silently change stored state, and a test that
// passed would be testing an aliasing accident.

// MemoryStore is an in-memory Store. It is safe for concurrent use.
type MemoryStore struct {
	mu sync.RWMutex

	runs      map[string]*Run
	steps     map[string]map[string]*StepState
	timers    map[string]*Timer
	subs      map[string]*Subscription
	joins     map[string]*Join
	leases    map[string]*Lease
	tasks     map[string]*Task
	events    []*Event
	sequences map[string]int64

	// maxEvents bounds the recorded event log so a long-lived test process does
	// not grow without limit.
	maxEvents int
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		runs:      map[string]*Run{},
		steps:     map[string]map[string]*StepState{},
		timers:    map[string]*Timer{},
		subs:      map[string]*Subscription{},
		joins:     map[string]*Join{},
		leases:    map[string]*Lease{},
		tasks:     map[string]*Task{},
		sequences: map[string]int64{},
		maxEvents: 1000,
	}
}

// Migrate implements Store. There is no schema to create.
func (m *MemoryStore) Migrate(context.Context) error { return nil }

// Close implements Store.
func (m *MemoryStore) Close() error { return nil }

// ---------------------------------------------------------------------------
// Runs
// ---------------------------------------------------------------------------

// CreateRun implements Store.
func (m *MemoryStore) CreateRun(_ context.Context, run *Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.runs[run.ID]; exists {
		return fmt.Errorf("ref/process: run %s already exists", run.ID)
	}
	run.Revision = 1
	m.runs[run.ID] = cloneRun(run)
	return nil
}

// GetRun implements Store.
func (m *MemoryStore) GetRun(_ context.Context, id string) (*Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	run, ok := m.runs[id]
	if !ok {
		return nil, ErrRunNotFound
	}
	return cloneRun(run), nil
}

// FindRunByIdempotency implements Store.
func (m *MemoryStore) FindRunByIdempotency(_ context.Context, process, key string) (*Run, error) {
	if key == "" {
		return nil, ErrRunNotFound
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var newest *Run
	for _, run := range m.runs {
		if run.Process != process || run.IdempotencyKey != key {
			continue
		}
		if newest == nil || run.CreatedAt.After(newest.CreatedAt) {
			newest = run
		}
	}
	if newest == nil {
		return nil, ErrRunNotFound
	}
	return cloneRun(newest), nil
}

// SaveRun implements Store, enforcing the same optimistic revision the SQL store
// does so tests exercise the real conflict path.
func (m *MemoryStore) SaveRun(_ context.Context, run *Run) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	stored, ok := m.runs[run.ID]
	if !ok {
		return ErrRunNotFound
	}
	if stored.Revision != run.Revision {
		return ErrRevisionConflict
	}
	run.Revision++
	run.UpdatedAt = time.Now().UTC()
	m.runs[run.ID] = cloneRun(run)
	return nil
}

// ListRuns implements Store.
func (m *MemoryStore) ListRuns(_ context.Context, filter RunFilter) ([]*Run, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now().UTC()
	var out []*Run
	for _, run := range m.runs {
		if !matchRun(run, filter, now) {
			continue
		}
		out = append(out, cloneRun(run))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return paginate(out, filter.Limit, filter.Offset), nil
}

// CountRuns implements Store.
func (m *MemoryStore) CountRuns(_ context.Context, filter RunFilter) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now().UTC()
	count := 0
	for _, run := range m.runs {
		if matchRun(run, filter, now) {
			count++
		}
	}
	return count, nil
}

func matchRun(run *Run, filter RunFilter, now time.Time) bool {
	switch {
	case filter.Process != "" && run.Process != filter.Process:
		return false
	case filter.Status != "" && run.Status != filter.Status:
		return false
	case filter.TenantID != "" && run.TenantID != filter.TenantID:
		return false
	case filter.Waiting && run.Status != StatusWaiting:
		return false
	case filter.Since != nil && run.CreatedAt.Before(*filter.Since):
		return false
	case filter.Overdue:
		if run.SLABreachAt == nil || run.SLABreachAt.After(now) || run.Status.Terminal() {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Steps
// ---------------------------------------------------------------------------

// SaveStep implements Store.
func (m *MemoryStore) SaveStep(_ context.Context, state *StepState) error {
	key := state.Key
	if key == "" {
		key = state.Step
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	states, ok := m.steps[state.RunID]
	if !ok {
		states = map[string]*StepState{}
		m.steps[state.RunID] = states
	}
	copied := *state
	copied.Key = key
	copied.Input = slices.Clone(state.Input)
	copied.Result = slices.Clone(state.Result)
	states[key] = &copied
	return nil
}

// ListSteps implements Store.
func (m *MemoryStore) ListSteps(_ context.Context, runID string) ([]*StepState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	states := m.steps[runID]
	out := make([]*StepState, 0, len(states))
	for _, state := range states {
		copied := *state
		copied.Input = slices.Clone(state.Input)
		copied.Result = slices.Clone(state.Result)
		out = append(out, &copied)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Sequence != out[j].Sequence {
			return out[i].Sequence < out[j].Sequence
		}
		return out[i].Key < out[j].Key
	})
	return out, nil
}

// NextStepSequence implements Store.
func (m *MemoryStore) NextStepSequence(_ context.Context, runID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sequences[runID]++
	return m.sequences[runID], nil
}

// ---------------------------------------------------------------------------
// Timers
// ---------------------------------------------------------------------------

// AddTimer implements Store.
func (m *MemoryStore) AddTimer(_ context.Context, timer *Timer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := *timer
	copied.Payload = slices.Clone(timer.Payload)
	m.timers[timer.ID] = &copied
	return nil
}

// DueTimers implements Store, removing what it returns so a timer fires once.
func (m *MemoryStore) DueTimers(_ context.Context, now time.Time, limit int) ([]*Timer, error) {
	if limit <= 0 {
		limit = 50
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var due []*Timer
	for _, timer := range m.timers {
		if !timer.Fire.After(now) {
			due = append(due, timer)
		}
	}
	sort.Slice(due, func(i, j int) bool { return due[i].Fire.Before(due[j].Fire) })
	if len(due) > limit {
		due = due[:limit]
	}
	out := make([]*Timer, 0, len(due))
	for _, timer := range due {
		delete(m.timers, timer.ID)
		copied := *timer
		out = append(out, &copied)
	}
	return out, nil
}

// DeleteTimer implements Store.
func (m *MemoryStore) DeleteTimer(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.timers, id)
	return nil
}

// DeleteRunTimers implements Store.
func (m *MemoryStore) DeleteRunTimers(_ context.Context, runID string, kinds ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, timer := range m.timers {
		if timer.RunID != runID {
			continue
		}
		if len(kinds) == 0 || slices.Contains(kinds, timer.Kind) {
			delete(m.timers, id)
		}
	}
	return nil
}

// ListTimers implements Store.
func (m *MemoryStore) ListTimers(_ context.Context, runID string) ([]*Timer, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Timer
	for _, timer := range m.timers {
		if timer.RunID == runID {
			copied := *timer
			out = append(out, &copied)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Fire.Before(out[j].Fire) })
	return out, nil
}

// ---------------------------------------------------------------------------
// Subscriptions
// ---------------------------------------------------------------------------

// Subscribe implements Store.
func (m *MemoryStore) Subscribe(_ context.Context, subscription *Subscription) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := *subscription
	m.subs[subscription.ID] = &copied
	return nil
}

// MatchSubscriptions implements Store.
func (m *MemoryStore) MatchSubscriptions(_ context.Context, event, correlation string) ([]*Subscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Subscription
	for _, subscription := range m.subs {
		if subscription.Event != event {
			continue
		}
		if subscription.Correlation != "" && subscription.Correlation != correlation {
			continue
		}
		copied := *subscription
		out = append(out, &copied)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// DeleteSubscription implements Store.
func (m *MemoryStore) DeleteSubscription(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subs, id)
	return nil
}

// DeleteRunSubscriptions implements Store.
func (m *MemoryStore) DeleteRunSubscriptions(_ context.Context, runID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, subscription := range m.subs {
		if subscription.RunID == runID {
			delete(m.subs, id)
		}
	}
	return nil
}

// ListSubscriptions implements Store.
func (m *MemoryStore) ListSubscriptions(_ context.Context, runID string) ([]*Subscription, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Subscription
	for _, subscription := range m.subs {
		if subscription.RunID == runID {
			copied := *subscription
			out = append(out, &copied)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// ---------------------------------------------------------------------------
// Joins
// ---------------------------------------------------------------------------

// SaveJoin implements Store.
func (m *MemoryStore) SaveJoin(_ context.Context, join *Join) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	join.UpdatedAt = time.Now().UTC()
	m.joins[join.RunID+"|"+join.Edge] = cloneJoin(join)
	return nil
}

// GetJoin implements Store.
func (m *MemoryStore) GetJoin(_ context.Context, runID, edge string) (*Join, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	join, ok := m.joins[runID+"|"+edge]
	if !ok {
		return nil, nil
	}
	return cloneJoin(join), nil
}

// ---------------------------------------------------------------------------
// Leases
// ---------------------------------------------------------------------------

// AcquireLease implements Store.
func (m *MemoryStore) AcquireLease(_ context.Context, runID, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	// Strictly exclusive, including against the same owner: one replica advancing a
	// run from two goroutines is exactly the interleaving this prevents, and a
	// replica that crashed and restarted under the same id cannot know its old self
	// is really gone — so it waits for the expiry like anybody else.
	if held, ok := m.leases[runID]; ok && held.ExpiresAt.After(now) {
		return false, nil
	}
	m.leases[runID] = &Lease{RunID: runID, Owner: owner, ExpiresAt: now.Add(ttl)}
	return true, nil
}

// RefreshLease implements Store.
func (m *MemoryStore) RefreshLease(_ context.Context, runID, owner string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	held, ok := m.leases[runID]
	if !ok || held.Owner != owner {
		return false, nil
	}
	held.ExpiresAt = time.Now().UTC().Add(ttl)
	return true, nil
}

// ReleaseLease implements Store.
func (m *MemoryStore) ReleaseLease(_ context.Context, runID, owner string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if held, ok := m.leases[runID]; ok && held.Owner == owner {
		delete(m.leases, runID)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Tasks
// ---------------------------------------------------------------------------

// SaveTask implements Store.
func (m *MemoryStore) SaveTask(_ context.Context, task *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if stored, ok := m.tasks[task.ID]; ok {
		if stored.Revision != task.Revision {
			return ErrRevisionConflict
		}
	} else if task.Revision != 0 {
		return ErrTaskNotFound
	}
	task.Revision++
	task.UpdatedAt = time.Now().UTC()
	m.tasks[task.ID] = cloneTask(task)
	return nil
}

// GetTask implements Store.
func (m *MemoryStore) GetTask(_ context.Context, id string) (*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, ok := m.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	return cloneTask(task), nil
}

// ListTasks implements Store.
func (m *MemoryStore) ListTasks(_ context.Context, filter TaskFilter) ([]*Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now().UTC()
	var out []*Task
	for _, task := range m.tasks {
		if !matchTask(task, filter, now) {
			continue
		}
		out = append(out, cloneTask(task))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority > out[j].Priority
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return paginate(out, filter.Limit, filter.Offset), nil
}

func matchTask(task *Task, filter TaskFilter, now time.Time) bool {
	if filter.Status != "" {
		if task.Status != filter.Status {
			return false
		}
	} else if !task.Open() {
		return false
	}
	switch {
	case filter.Process != "" && task.Process != filter.Process:
		return false
	case filter.RunID != "" && task.RunID != filter.RunID:
		return false
	case filter.TenantID != "" && task.TenantID != filter.TenantID:
		return false
	case filter.Queue != "" && task.Queue != filter.Queue:
		return false
	case filter.Overdue && (task.DueAt == nil || task.DueAt.After(now)):
		return false
	}
	if filter.Assignee == "" && len(filter.Roles) == 0 {
		return true
	}
	if filter.Assignee != "" && (task.Assignee == filter.Assignee || task.ClaimedBy == filter.Assignee) {
		return true
	}
	// An unassigned task addressed to one of the viewer's roles is theirs to claim.
	return task.Assignee == "" && slices.Contains(filter.Roles, task.Role)
}

// CountOpenTasks implements Store.
func (m *MemoryStore) CountOpenTasks(_ context.Context, assignee string) (int, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	count := 0
	for _, task := range m.tasks {
		if task.Open() && (task.Assignee == assignee || task.ClaimedBy == assignee) {
			count++
		}
	}
	return count, nil
}

// DueTasks implements Store.
func (m *MemoryStore) DueTasks(_ context.Context, now time.Time, limit int) ([]*Task, error) {
	if limit <= 0 {
		limit = 50
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*Task
	for _, task := range m.tasks {
		if task.Status != TaskOpen && task.Status != TaskClaimed {
			continue
		}
		due := task.DueAt != nil && !task.DueAt.After(now)
		reminder := task.ReminderAt != nil && !task.ReminderAt.After(now)
		if due || reminder {
			out = append(out, cloneTask(task))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		switch {
		case out[i].DueAt == nil:
			return false
		case out[j].DueAt == nil:
			return true
		default:
			return out[i].DueAt.Before(*out[j].DueAt)
		}
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// Events and retention
// ---------------------------------------------------------------------------

// RecordEvent implements Store.
func (m *MemoryStore) RecordEvent(_ context.Context, event *Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := *event
	copied.Payload = slices.Clone(event.Payload)
	m.events = append(m.events, &copied)
	if len(m.events) > m.maxEvents {
		m.events = m.events[len(m.events)-m.maxEvents:]
	}
	return nil
}

// PurgeRuns implements Store.
func (m *MemoryStore) PurgeRuns(_ context.Context, process string, before time.Time, limit int) (int, error) {
	if limit <= 0 {
		limit = 200
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	purged := 0
	for id, run := range m.runs {
		if purged >= limit {
			break
		}
		if !run.Status.Terminal() || run.CompletedAt == nil || !run.CompletedAt.Before(before) {
			continue
		}
		if process != "" && run.Process != process {
			continue
		}
		delete(m.runs, id)
		delete(m.steps, id)
		delete(m.sequences, id)
		delete(m.leases, id)
		for timerID, timer := range m.timers {
			if timer.RunID == id {
				delete(m.timers, timerID)
			}
		}
		for subID, subscription := range m.subs {
			if subscription.RunID == id {
				delete(m.subs, subID)
			}
		}
		for joinID := range m.joins {
			if strings.HasPrefix(joinID, id+"|") {
				delete(m.joins, joinID)
			}
		}
		for taskID, task := range m.tasks {
			if task.RunID == id {
				delete(m.tasks, taskID)
			}
		}
		purged++
	}
	return purged, nil
}

// ---------------------------------------------------------------------------
// Cloning
// ---------------------------------------------------------------------------

func cloneRun(run *Run) *Run {
	copied := *run
	copied.Input = slices.Clone(run.Input)
	copied.Output = slices.Clone(run.Output)
	copied.Frames = make([]Frame, len(run.Frames))
	for i, frame := range run.Frames {
		copied.Frames[i] = frame
		copied.Frames[i].Input = slices.Clone(frame.Input)
	}
	copied.Compensating = slices.Clone(run.Compensating)
	if run.Visits != nil {
		copied.Visits = make(map[string]int, len(run.Visits))
		for key, value := range run.Visits {
			copied.Visits[key] = value
		}
	}
	if run.Waiting != nil {
		waiting := *run.Waiting
		copied.Waiting = &waiting
	}
	copied.StartedAt = cloneTime(run.StartedAt)
	copied.CompletedAt = cloneTime(run.CompletedAt)
	copied.DeadlineAt = cloneTime(run.DeadlineAt)
	copied.SLATargetAt = cloneTime(run.SLATargetAt)
	copied.SLABreachAt = cloneTime(run.SLABreachAt)
	return &copied
}

func cloneJoin(join *Join) *Join {
	copied := *join
	copied.Sources = slices.Clone(join.Sources)
	if join.Results != nil {
		copied.Results = make(map[string]json.RawMessage, len(join.Results))
		for key, value := range join.Results {
			copied.Results[key] = slices.Clone(value)
		}
	}
	if join.Errors != nil {
		copied.Errors = make(map[string]string, len(join.Errors))
		for key, value := range join.Errors {
			copied.Errors[key] = value
		}
	}
	return &copied
}

func cloneTask(task *Task) *Task {
	copied := *task
	copied.Skills = slices.Clone(task.Skills)
	copied.ForbidPrincipals = slices.Clone(task.ForbidPrincipals)
	copied.Actions = slices.Clone(task.Actions)
	copied.Data = slices.Clone(task.Data)
	copied.Result = slices.Clone(task.Result)
	copied.ClaimedAt = cloneTime(task.ClaimedAt)
	copied.CompletedAt = cloneTime(task.CompletedAt)
	copied.DueAt = cloneTime(task.DueAt)
	copied.ReminderAt = cloneTime(task.ReminderAt)
	return &copied
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copied := *value
	return &copied
}

func paginate[T any](items []T, limit, offset int) []T {
	if offset > 0 {
		if offset >= len(items) {
			return nil
		}
		items = items[offset:]
	}
	if limit <= 0 {
		limit = 50
	}
	if len(items) > limit {
		items = items[:limit]
	}
	return items
}

// Interface assertion: a drift between the memory and SQL stores becomes a
// compile error here rather than a test that only runs against one of them.
var _ Store = (*MemoryStore)(nil)

// ErrNotImplemented is returned by optional store capabilities a backend does not
// provide.
var ErrNotImplemented = errors.New("ref/process: not implemented by this store")
