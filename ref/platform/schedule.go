package platform

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"time"

	"github.com/oarkflow/fh"
	"github.com/oarkflow/fh/ref/platform/spi"
	"github.com/oarkflow/fh/ref/process"
)

// Schedules and triggers: the non-request entry points.
//
// A schedule does not run work in a ticker goroutine. It enqueues it, and whichever
// replica claims the job runs it. That distinction is the whole design: an
// in-process ticker on three replicas fires three times, and a ticker on a replica
// that is restarting does not fire at all. A queued occurrence fires exactly once
// and survives the restart.
//
// The cron parser here is deliberately the standard five-field form and nothing
// more. No seconds field, no @every, no step-of-range beyond the ordinary `*/n` —
// because every extension is another thing to get subtly wrong, and a deployment
// that needs sub-minute scheduling wants a queue, not a crontab.

// compiledSchedule is one schedule with its timing resolved.
type compiledSchedule struct {
	spec     ScheduleSpec
	queue    spi.JobQueue
	delayed  spi.QueueDelay
	cron     *cronSchedule
	location *time.Location
	jobType  string
	// every and jitter are the parsed forms of the spec's duration strings.
	every  time.Duration
	jitter time.Duration
	// next is the occurrence this replica is waiting for, recomputed after each
	// firing.
	next time.Time
}

// compileSchedules resolves each schedule's queue and timing.
func (p *Platform) compileSchedules(doc Document) error {
	for _, spec := range doc.Schedules {
		if spec.Disabled {
			continue
		}
		queue, ok := p.resources[spec.Queue].(spi.JobQueue)
		if !ok {
			return fmt.Errorf("ref/platform: schedule %q: resource %q is not a queue", spec.Name, spec.Queue)
		}
		every, err := durationField("schedule "+spec.Name, "every", spec.Every, 0)
		if err != nil {
			return err
		}
		jitter, err := durationField("schedule "+spec.Name, "jitter", spec.Jitter, 0)
		if err != nil {
			return err
		}
		compiled := compiledSchedule{
			spec:     spec,
			queue:    queue,
			location: time.UTC,
			jobType:  "platform.schedule." + spec.Name,
			every:    every,
			jitter:   jitter,
		}
		compiled.delayed, _ = queue.(spi.QueueDelay)
		if spec.Timezone != "" {
			location, err := time.LoadLocation(spec.Timezone)
			if err != nil {
				return fmt.Errorf("ref/platform: schedule %q timezone: %w", spec.Name, err)
			}
			compiled.location = location
		}
		if spec.Cron != "" {
			parsed, err := parseCron(spec.Cron)
			if err != nil {
				return fmt.Errorf("ref/platform: schedule %q cron: %w", spec.Name, err)
			}
			compiled.cron = parsed
		}

		// The handler is registered on the queue so any replica can run an
		// occurrence, whichever one enqueued it.
		if consumer, ok := p.resources[spec.Queue].(workerQueue); ok {
			schedule := spec
			consumer.Register(compiled.jobType, func(ctx context.Context, job *fh.QueueJob) error {
				return p.runScheduled(ctx, schedule, job.Payload)
			})
		}
		p.schedules = append(p.schedules, compiled)
	}
	return nil
}

// runScheduled executes one occurrence.
func (p *Platform) runScheduled(ctx context.Context, spec ScheduleSpec, payload []byte) error {
	input := any(spec.Payload)
	if len(payload) > 0 {
		var decoded any
		if json.Unmarshal(payload, &decoded) == nil && decoded != nil {
			input = decoded
		}
	}
	if spec.Process != "" {
		engine, ok := p.processes[spec.Process]
		if !ok {
			return fmt.Errorf("ref/platform: schedule %q names unknown process %q", spec.Name, spec.Process)
		}
		_, err := engine.Start(ctx, spec.Process, input, process.StartOptions{
			TenantID: spec.TenantID,
			Detached: true,
		})
		return err
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return err
	}
	return p.dispatchInternal(ctx, spec.Intent, encoded, "", spec.TenantID)
}

// runSchedules is the loop that enqueues occurrences.
//
// Every replica runs it, and every replica enqueues — which would mean N jobs per
// occurrence. The deduplication is the job's own idempotency key: the occurrence's
// instant. Whichever replica gets there first creates the job; the rest collide on
// the key and do nothing.
func (p *Platform) runSchedules(ctx context.Context) {
	for i := range p.schedules {
		p.schedules[i].next = p.schedules[i].firstOccurrence(time.Now())
	}
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for i := range p.schedules {
				schedule := &p.schedules[i]
				if schedule.next.IsZero() || now.Before(schedule.next) {
					continue
				}
				p.enqueueOccurrence(ctx, schedule, schedule.next)
				schedule.next = schedule.nextOccurrence(schedule.next)
			}
		}
	}
}

// enqueueOccurrence publishes one occurrence, keyed so only one replica's job
// survives.
func (p *Platform) enqueueOccurrence(ctx context.Context, schedule *compiledSchedule, occurrence time.Time) {
	payload, err := json.Marshal(schedule.spec.Payload)
	if err != nil {
		return
	}
	headers := map[string]string{
		"schedule":   schedule.spec.Name,
		"occurrence": occurrence.UTC().Format(time.RFC3339),
		"tenant_id":  schedule.spec.TenantID,
	}

	// Jitter spreads a fleet's identical schedules so they do not all hit the same
	// dependency in the same instant.
	runAt := occurrence
	if schedule.jitter > 0 {
		runAt = occurrence.Add(time.Duration(rand.Int64N(int64(schedule.jitter))))
	}
	if schedule.delayed != nil && runAt.After(time.Now()) {
		_, _ = schedule.delayed.EnqueueDelayed(schedule.jobType, json.RawMessage(payload), runAt, headers)
		return
	}
	_, _ = schedule.queue.Enqueue(schedule.jobType, json.RawMessage(payload), headers)
}

// firstOccurrence is the first firing at or after now.
func (s *compiledSchedule) firstOccurrence(now time.Time) time.Time {
	switch {
	case s.spec.At != "":
		instant, err := time.Parse(time.RFC3339, s.spec.At)
		if err != nil || instant.Before(now) {
			// A one-shot instant in the past never fires. Firing it late would be
			// surprising for a schedule whose whole meaning was "at this moment".
			return time.Time{}
		}
		return instant
	case s.cron != nil:
		return s.cron.next(now.In(s.location))
	case s.every > 0:
		return now.Add(s.every)
	default:
		return time.Time{}
	}
}

// nextOccurrence is the firing after the one just handled.
func (s *compiledSchedule) nextOccurrence(previous time.Time) time.Time {
	switch {
	case s.spec.At != "":
		// One-shot: there is no next.
		return time.Time{}
	case s.cron != nil:
		return s.cron.next(previous.In(s.location).Add(time.Minute))
	case s.every > 0:
		next := previous.Add(s.every)
		// A replica that was paused must not fire a backlog of occurrences at once,
		// so a missed window is skipped forward rather than replayed.
		if now := time.Now(); next.Before(now) {
			next = now.Add(s.every)
		}
		return next
	default:
		return time.Time{}
	}
}

// dispatchInternal runs an intent outside any HTTP request, for schedules and
// triggers.
func (p *Platform) dispatchInternal(ctx context.Context, intentName string, body []byte, principalID, tenant string) error {
	ctx = withProcessIdentity(ctx, principalID, tenant)
	inv := newInternalInvocation(intentName, body, principalID, tenant)
	_, err := p.Engine.Dispatch(ctx, inv)
	return err
}

// ---------------------------------------------------------------------------
// Cron
// ---------------------------------------------------------------------------

// cronSchedule is a parsed five-field crontab expression: minute, hour, day of
// month, month, day of week.
type cronSchedule struct {
	minutes  []bool // 60
	hours    []bool // 24
	days     []bool // 32, 1-based
	months   []bool // 13, 1-based
	weekdays []bool // 7, Sunday = 0
	// dayRestricted and weekdayRestricted record whether each field was specified,
	// because crontab's day-of-month and day-of-week fields are OR-ed when both are
	// restricted — a rule that surprises people but is the actual standard.
	dayRestricted     bool
	weekdayRestricted bool
}

// parseCron parses a five-field crontab expression.
func parseCron(expression string) (*cronSchedule, error) {
	fields := strings.Fields(expression)
	if len(fields) != 5 {
		return nil, fmt.Errorf("a cron expression needs five fields (minute hour day month weekday), got %d", len(fields))
	}
	schedule := &cronSchedule{}
	var err error
	if schedule.minutes, err = parseCronField(fields[0], 0, 59); err != nil {
		return nil, fmt.Errorf("minute: %w", err)
	}
	if schedule.hours, err = parseCronField(fields[1], 0, 23); err != nil {
		return nil, fmt.Errorf("hour: %w", err)
	}
	if schedule.days, err = parseCronField(fields[2], 1, 31); err != nil {
		return nil, fmt.Errorf("day of month: %w", err)
	}
	if schedule.months, err = parseCronField(fields[3], 1, 12); err != nil {
		return nil, fmt.Errorf("month: %w", err)
	}
	if schedule.weekdays, err = parseCronField(fields[4], 0, 6); err != nil {
		return nil, fmt.Errorf("day of week: %w", err)
	}
	schedule.dayRestricted = fields[2] != "*"
	schedule.weekdayRestricted = fields[4] != "*"
	return schedule, nil
}

// parseCronField parses one field into a set over [low, high].
func parseCronField(field string, low, high int) ([]bool, error) {
	set := make([]bool, high+1)
	for _, part := range strings.Split(field, ",") {
		step := 1
		if slash := strings.Index(part, "/"); slash >= 0 {
			parsed, err := strconv.Atoi(part[slash+1:])
			if err != nil || parsed <= 0 {
				return nil, fmt.Errorf("%q has an invalid step", part)
			}
			step = parsed
			part = part[:slash]
		}
		start, end := low, high
		switch {
		case part == "*" || part == "":
		case strings.Contains(part, "-"):
			bounds := strings.SplitN(part, "-", 2)
			var err error
			if start, err = strconv.Atoi(bounds[0]); err != nil {
				return nil, fmt.Errorf("%q is not a number", bounds[0])
			}
			if end, err = strconv.Atoi(bounds[1]); err != nil {
				return nil, fmt.Errorf("%q is not a number", bounds[1])
			}
		default:
			value, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("%q is not a number", part)
			}
			start, end = value, value
		}
		if start < low || end > high || start > end {
			return nil, fmt.Errorf("%q is outside the range %d-%d", part, low, high)
		}
		for value := start; value <= end; value += step {
			set[value] = true
		}
	}
	return set, nil
}

// next returns the first matching instant strictly after from.
//
// The search is bounded: four years of minutes is enough to find any occurrence a
// valid five-field expression can have, and a bound means an impossible expression
// (31 February) returns nothing rather than looping.
func (c *cronSchedule) next(from time.Time) time.Time {
	candidate := from.Truncate(time.Minute).Add(time.Minute)
	limit := candidate.AddDate(4, 0, 0)
	for candidate.Before(limit) {
		if c.matches(candidate) {
			return candidate
		}
		candidate = candidate.Add(time.Minute)
	}
	return time.Time{}
}

func (c *cronSchedule) matches(instant time.Time) bool {
	if !c.minutes[instant.Minute()] || !c.hours[instant.Hour()] || !c.months[int(instant.Month())] {
		return false
	}
	day := c.days[instant.Day()]
	weekday := c.weekdays[int(instant.Weekday())]
	switch {
	case c.dayRestricted && c.weekdayRestricted:
		// Crontab OR-s these two when both are restricted.
		return day || weekday
	case c.dayRestricted:
		return day
	case c.weekdayRestricted:
		return weekday
	default:
		return true
	}
}
