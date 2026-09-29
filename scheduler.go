package worker

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// OverlapPolicy defines behavior when a task is still running at its next scheduled time.
type OverlapPolicy int

const (
	// OverlapSkip skips the new execution if the previous is still running (default).
	OverlapSkip OverlapPolicy = iota
	// OverlapAllow allows concurrent executions.
	OverlapAllow
	// OverlapQueue queues the new execution to run after the current completes.
	OverlapQueue
)

// Schedule determines when a task should next run.
type Schedule interface {
	// Next returns the next execution time after the given time.
	// A zero time means the schedule has no more executions.
	Next(after time.Time) time.Time
}

// IntervalSchedule fires at fixed intervals.
type IntervalSchedule struct {
	Interval time.Duration
}

// Next implements [Schedule].
func (s *IntervalSchedule) Next(after time.Time) time.Time {
	return after.Add(s.Interval)
}

// FixedTimeSchedule fires at a start time, then repeats at a fixed interval.
type FixedTimeSchedule struct {
	Start    time.Time
	Interval time.Duration
}

// Next implements [Schedule].
func (s *FixedTimeSchedule) Next(after time.Time) time.Time {
	if after.Before(s.Start) {
		return s.Start
	}
	elapsed := after.Sub(s.Start)
	periods := int64(elapsed / s.Interval)
	return s.Start.Add(time.Duration(periods+1) * s.Interval)
}

// OnceSchedule fires exactly once at a given time.
type OnceSchedule struct {
	At   time.Time
	done atomic.Bool
}

// Next implements [Schedule].
func (s *OnceSchedule) Next(after time.Time) time.Time {
	if s.done.Load() {
		return time.Time{}
	}
	if after.After(s.At) {
		s.done.Store(true)
		return time.Time{}
	}
	return s.At
}

// CronSchedule fires based on cron-like expressions.
//
// Format: "minute hour day-of-month month day-of-week"
//
// Supports *, */n, n-m, and comma-separated lists.
type CronSchedule struct {
	minutes     []int
	hours       []int
	days        []int
	months      []int
	weekdays    []int
	expression  string
	allDays     bool // true when the day-of-month field was "*"
	allWeekdays bool // true when the day-of-week field was "*"
}

// ParseCron parses a standard 5-field cron expression.
//
// Examples:
//
//	"*/15 * * * *"  — every 15 minutes
//	"0 9 * * 1-5"  — 9 AM on weekdays
//	"0 0 1 * *"    — midnight on the 1st of each month
func ParseCron(expr string) (*CronSchedule, error) {
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid cron expression: expected 5 fields, got %d", len(parts))
	}

	minutes, err := parseCronField(parts[0], 0, 59)
	if err != nil {
		return nil, fmt.Errorf("minute field: %w", err)
	}
	hours, err := parseCronField(parts[1], 0, 23)
	if err != nil {
		return nil, fmt.Errorf("hour field: %w", err)
	}
	days, err := parseCronField(parts[2], 1, 31)
	if err != nil {
		return nil, fmt.Errorf("day field: %w", err)
	}
	months, err := parseCronField(parts[3], 1, 12)
	if err != nil {
		return nil, fmt.Errorf("month field: %w", err)
	}
	weekdays, err := parseCronField(parts[4], 0, 6)
	if err != nil {
		return nil, fmt.Errorf("weekday field: %w", err)
	}

	return &CronSchedule{
		minutes:     minutes,
		hours:       hours,
		days:        days,
		months:      months,
		weekdays:    weekdays,
		expression:  expr,
		allDays:     parts[2] == "*",
		allWeekdays: parts[4] == "*",
	}, nil
}

// Next implements [Schedule].
func (s *CronSchedule) Next(after time.Time) time.Time {
	// Truncate to the next whole minute in the same timezone.
	t := after.Add(time.Minute)
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	limit := after.Add(5 * 365 * 24 * time.Hour)

	for t.Before(limit) {
		if !slices.Contains(s.months, int(t.Month())) {
			t = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
			continue
		}

		// Standard cron: if both day-of-month and day-of-week are restricted
		// (not "*"), match on EITHER. If only one is restricted, use only that one.
		dayMatch := s.allDays || slices.Contains(s.days, t.Day())
		wdayMatch := s.allWeekdays || slices.Contains(s.weekdays, int(t.Weekday()))

		var dayOk bool
		switch {
		case s.allDays && s.allWeekdays:
			dayOk = true // both unrestricted
		case !s.allDays && !s.allWeekdays:
			dayOk = dayMatch || wdayMatch // OR semantics per standard cron
		default:
			dayOk = dayMatch && wdayMatch // one is "*" so check the restricted one
		}

		if !dayOk {
			t = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
			continue
		}
		if !slices.Contains(s.hours, t.Hour()) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
			continue
		}
		if !slices.Contains(s.minutes, t.Minute()) {
			t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, t.Location())
			continue
		}
		return t
	}
	return time.Time{}
}

// String returns the original cron expression.
func (s *CronSchedule) String() string { return s.expression }

// DailySchedule fires at specific HH:MM times each day.
type DailySchedule struct {
	times []time.Duration // offsets from midnight, sorted ascending
}

// NewDailySchedule creates a schedule from "HH:MM" strings.
func NewDailySchedule(times ...string) (*DailySchedule, error) {
	durations := make([]time.Duration, 0, len(times))
	for _, t := range times {
		parts := strings.Split(t, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid time format %q (expected HH:MM)", t)
		}
		h, err := strconv.Atoi(parts[0])
		if err != nil || h < 0 || h > 23 {
			return nil, fmt.Errorf("invalid hour in %q", t)
		}
		m, err := strconv.Atoi(parts[1])
		if err != nil || m < 0 || m > 59 {
			return nil, fmt.Errorf("invalid minute in %q", t)
		}
		durations = append(durations, time.Duration(h)*time.Hour+time.Duration(m)*time.Minute)
	}
	slices.Sort(durations)
	return &DailySchedule{times: durations}, nil
}

// Next implements [Schedule].
func (s *DailySchedule) Next(after time.Time) time.Time {
	midnight := time.Date(after.Year(), after.Month(), after.Day(), 0, 0, 0, 0, after.Location())
	since := after.Sub(midnight)

	for _, t := range s.times {
		if t > since {
			return midnight.Add(t)
		}
	}
	if len(s.times) > 0 {
		return midnight.Add(24 * time.Hour).Add(s.times[0])
	}
	return time.Time{}
}

// --- Scheduler ---

// defaultTickInterval is the default scheduler tick resolution.
const defaultTickInterval = 100 * time.Millisecond

// maxQueuedPerTask limits how many executions can be queued per task with OverlapQueue.
const maxQueuedPerTask int32 = 100

// Scheduler manages periodic task execution on a worker pool.
type Scheduler struct {
	pool    *Pool
	tasks   map[string]*scheduledTask
	tasksMu sync.RWMutex
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	logger  Logger
	running atomic.Bool

	tickInterval time.Duration
	onTaskStart  func(name string)
	onTaskEnd    func(name string, err error, duration time.Duration)
}

type scheduledTask struct {
	name     string
	job      Job
	schedule Schedule
	next     time.Time
	running  atomic.Int32
	overlap  OverlapPolicy
	queued   atomic.Int32

	mu     sync.Mutex // protects cancel and stats fields
	cancel context.CancelFunc

	paused  atomic.Bool
	maxRuns int
	runCnt  atomic.Int64
	jitter  time.Duration
	runNow  bool

	// stats — protected by mu
	lastRun     time.Time
	lastErr     error
	lastDur     time.Duration
	totalRuns   int64
	totalErrors int64
}

// SchedulerOption configures a [Scheduler].
type SchedulerOption func(*Scheduler)

// WithSchedulerLogger sets the scheduler's logger.
func WithSchedulerLogger(l Logger) SchedulerOption {
	return func(s *Scheduler) { s.logger = l }
}

// WithTickInterval sets the scheduler's tick resolution. Default: 100ms.
// Lower values give more precise scheduling but use more CPU.
func WithTickInterval(d time.Duration) SchedulerOption {
	return func(s *Scheduler) {
		if d > 0 {
			s.tickInterval = d
		}
	}
}

// WithOnTaskStart sets a callback invoked when a task begins execution.
func WithOnTaskStart(fn func(name string)) SchedulerOption {
	return func(s *Scheduler) { s.onTaskStart = fn }
}

// WithOnTaskEnd sets a callback invoked when a task finishes.
func WithOnTaskEnd(fn func(name string, err error, d time.Duration)) SchedulerOption {
	return func(s *Scheduler) { s.onTaskEnd = fn }
}

// TaskOption configures an individual scheduled task.
type TaskOption func(*scheduledTask)

// WithMaxRuns limits the number of executions (0 = unlimited).
func WithMaxRuns(n int) TaskOption {
	return func(t *scheduledTask) { t.maxRuns = n }
}

// WithJitter adds random jitter to the schedule to prevent thundering herd.
func WithJitter(d time.Duration) TaskOption {
	return func(t *scheduledTask) { t.jitter = d }
}

// WithOverlapPolicy sets the behavior when a task is still running at its next
// scheduled time. Default: [OverlapSkip].
func WithOverlapPolicy(p OverlapPolicy) TaskOption {
	return func(t *scheduledTask) { t.overlap = p }
}

// WithRunImmediate causes the task to fire immediately when the scheduler starts.
func WithRunImmediate() TaskOption {
	return func(t *scheduledTask) { t.runNow = true }
}

// NewScheduler creates a new scheduler that dispatches work to pool.
func NewScheduler(pool *Pool, opts ...SchedulerOption) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		pool:         pool,
		tasks:        make(map[string]*scheduledTask),
		ctx:          ctx,
		cancel:       cancel,
		tickInterval: defaultTickInterval,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Schedule adds a job with a custom [Schedule].
func (s *Scheduler) Schedule(name string, sched Schedule, job Job, opts ...TaskOption) *Scheduler {
	task := &scheduledTask{
		name:     name,
		job:      job,
		schedule: sched,
		next:     sched.Next(time.Now()),
		overlap:  OverlapSkip,
	}
	for _, o := range opts {
		o(task)
	}
	if task.runNow {
		task.next = time.Now()
	}
	s.tasksMu.Lock()
	s.tasks[name] = task
	s.tasksMu.Unlock()
	return s
}

// ScheduleFunc is a convenience wrapper around [Scheduler.Schedule].
func (s *Scheduler) ScheduleFunc(name string, sched Schedule, fn func(context.Context) error, opts ...TaskOption) *Scheduler {
	return s.Schedule(name, sched, JobFunc(fn), opts...)
}

// Every schedules a job at a fixed interval.
func (s *Scheduler) Every(name string, interval time.Duration, job Job, opts ...TaskOption) *Scheduler {
	return s.Schedule(name, &IntervalSchedule{Interval: interval}, job, opts...)
}

// EveryFunc is a convenience wrapper around [Scheduler.Every].
func (s *Scheduler) EveryFunc(name string, interval time.Duration, fn func(context.Context) error, opts ...TaskOption) *Scheduler {
	return s.Every(name, interval, JobFunc(fn), opts...)
}

// At schedules a job starting at a specific time, then repeating at interval.
func (s *Scheduler) At(name string, start time.Time, interval time.Duration, job Job, opts ...TaskOption) *Scheduler {
	return s.Schedule(name, &FixedTimeSchedule{Start: start, Interval: interval}, job, opts...)
}

// Once schedules a job to run exactly once at the given time.
func (s *Scheduler) Once(name string, at time.Time, job Job, opts ...TaskOption) *Scheduler {
	return s.Schedule(name, &OnceSchedule{At: at}, job, opts...)
}

// OnceFunc is a convenience wrapper around [Scheduler.Once].
func (s *Scheduler) OnceFunc(name string, at time.Time, fn func(context.Context) error, opts ...TaskOption) *Scheduler {
	return s.Once(name, at, JobFunc(fn), opts...)
}

// Cron schedules a job using a cron expression.
func (s *Scheduler) Cron(name, expr string, job Job, opts ...TaskOption) error {
	sched, err := ParseCron(expr)
	if err != nil {
		return err
	}
	s.Schedule(name, sched, job, opts...)
	return nil
}

// CronFunc is a convenience wrapper around [Scheduler.Cron].
func (s *Scheduler) CronFunc(name, expr string, fn func(context.Context) error, opts ...TaskOption) error {
	return s.Cron(name, expr, JobFunc(fn), opts...)
}

// Daily schedules a job at specific "HH:MM" times each day.
func (s *Scheduler) Daily(name string, times []string, job Job, opts ...TaskOption) error {
	sched, err := NewDailySchedule(times...)
	if err != nil {
		return err
	}
	s.Schedule(name, sched, job, opts...)
	return nil
}

// DailyFunc is a convenience wrapper around [Scheduler.Daily].
func (s *Scheduler) DailyFunc(name string, times []string, fn func(context.Context) error, opts ...TaskOption) error {
	return s.Daily(name, times, JobFunc(fn), opts...)
}

// Remove cancels and removes a scheduled task.
func (s *Scheduler) Remove(name string) {
	s.tasksMu.Lock()
	defer s.tasksMu.Unlock()
	if t, ok := s.tasks[name]; ok {
		t.mu.Lock()
		if t.cancel != nil {
			t.cancel()
		}
		t.mu.Unlock()
		delete(s.tasks, name)
	}
}

// Pause pauses a task by name. Returns false if the task does not exist.
func (s *Scheduler) Pause(name string) bool {
	s.tasksMu.RLock()
	t, ok := s.tasks[name]
	s.tasksMu.RUnlock()
	if !ok {
		return false
	}
	t.paused.Store(true)
	return true
}

// Resume resumes a paused task. Returns false if the task does not exist.
func (s *Scheduler) Resume(name string) bool {
	s.tasksMu.RLock()
	t, ok := s.tasks[name]
	s.tasksMu.RUnlock()
	if !ok {
		return false
	}
	t.paused.Store(false)
	return true
}

// Trigger manually fires a task immediately, outside its normal schedule.
func (s *Scheduler) Trigger(name string) bool {
	s.tasksMu.RLock()
	t, ok := s.tasks[name]
	s.tasksMu.RUnlock()
	if !ok {
		return false
	}
	go s.executeTask(t)
	return true
}

// Start begins the scheduler loop. It is idempotent.
func (s *Scheduler) Start() {
	if s.running.Swap(true) {
		return
	}
	if s.logger != nil {
		s.logger.Info("scheduler started")
	}
	s.wg.Add(1)
	go s.loop()
}

func (s *Scheduler) loop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			s.tick(now)
		}
	}
}

func (s *Scheduler) tick(now time.Time) {
	s.tasksMu.RLock()
	snapshot := make([]*scheduledTask, 0, len(s.tasks))
	for _, t := range s.tasks {
		snapshot = append(snapshot, t)
	}
	s.tasksMu.RUnlock()

	for _, task := range snapshot {
		if task.paused.Load() {
			continue
		}
		if task.maxRuns > 0 && task.runCnt.Load() >= int64(task.maxRuns) {
			continue
		}
		if task.next.IsZero() || now.Before(task.next) {
			continue
		}

		isRunning := task.running.Load() > 0
		if isRunning {
			switch task.overlap {
			case OverlapSkip:
				s.tasksMu.Lock()
				task.next = task.schedule.Next(now)
				s.tasksMu.Unlock()
				continue
			case OverlapQueue:
				if task.queued.Load() < maxQueuedPerTask {
					task.queued.Add(1)
				}
				continue
			case OverlapAllow:
				// fall through
			}
		}

		// Schedule next
		s.tasksMu.Lock()
		next := task.schedule.Next(now)
		if task.jitter > 0 {
			next = next.Add(time.Duration(rand.Int63n(int64(task.jitter)))) //nolint:gosec // scheduling jitter is not security-sensitive
		}
		task.next = next
		s.tasksMu.Unlock()

		go s.executeTask(task)
	}
}

func (s *Scheduler) executeTask(task *scheduledTask) {
	task.running.Add(1)
	task.runCnt.Add(1)
	defer func() {
		task.running.Add(-1)
		if task.queued.Load() > 0 {
			task.queued.Add(-1)
			go s.executeTask(task)
		}
	}()

	if s.logger != nil {
		s.logger.Debug("running task", "name", task.name)
	}
	if s.onTaskStart != nil {
		s.onTaskStart(task.name)
	}

	ctx, cancel := context.WithCancel(s.ctx)
	task.mu.Lock()
	task.cancel = cancel
	task.mu.Unlock()

	start := time.Now()
	err := s.pool.SubmitJobWait(ctx, task.job)
	dur := time.Since(start)

	task.mu.Lock()
	task.lastRun = start
	task.lastErr = err
	task.lastDur = dur
	task.totalRuns++
	if err != nil {
		task.totalErrors++
	}
	task.mu.Unlock()

	if err != nil && s.logger != nil {
		s.logger.Error("task failed", "name", task.name, "error", err, "duration", dur)
	}
	if s.onTaskEnd != nil {
		s.onTaskEnd(task.name, err, dur)
	}
}

// Stop stops the scheduler and waits for the loop goroutine to exit.
// In-flight tasks submitted to the pool continue independently.
func (s *Scheduler) Stop() {
	if !s.running.Swap(false) {
		return
	}
	s.cancel()
	s.wg.Wait()
	if s.logger != nil {
		s.logger.Info("scheduler stopped")
	}
}

// IsRunning reports whether the scheduler loop is active.
func (s *Scheduler) IsRunning() bool {
	return s.running.Load()
}

// Tasks returns the names of all registered tasks.
func (s *Scheduler) Tasks() []string {
	s.tasksMu.RLock()
	defer s.tasksMu.RUnlock()
	names := make([]string, 0, len(s.tasks))
	for name := range s.tasks {
		names = append(names, name)
	}
	return names
}

// TaskInfo returns information about a named task, or nil if not found.
func (s *Scheduler) TaskInfo(name string) *TaskInfo {
	s.tasksMu.RLock()
	t, ok := s.tasks[name]
	s.tasksMu.RUnlock()
	if !ok {
		return nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	return &TaskInfo{
		Name:         t.name,
		Next:         t.next,
		Running:      t.running.Load() > 0,
		Paused:       t.paused.Load(),
		RunCount:     t.runCnt.Load(),
		MaxRuns:      t.maxRuns,
		LastRun:      t.lastRun,
		LastError:    t.lastErr,
		LastDuration: t.lastDur,
		TotalRuns:    t.totalRuns,
		TotalErrors:  t.totalErrors,
	}
}

// TaskInfo is a snapshot of a scheduled task's state.
type TaskInfo struct {
	Name         string
	Next         time.Time
	Running      bool
	Paused       bool
	RunCount     int64
	MaxRuns      int
	LastRun      time.Time
	LastError    error
	LastDuration time.Duration
	TotalRuns    int64
	TotalErrors  int64
}

// SuccessRate returns the fraction of successful runs (0.0–1.0).
func (i *TaskInfo) SuccessRate() float64 {
	if i.TotalRuns == 0 {
		return 0
	}
	return float64(i.TotalRuns-i.TotalErrors) / float64(i.TotalRuns)
}

// --- helpers ---

func parseCronField(field string, min, max int) ([]int, error) {
	var result []int
	for part := range strings.SplitSeq(field, ",") {
		part = strings.TrimSpace(part)
		switch {
		case part == "*":
			for i := min; i <= max; i++ {
				result = append(result, i)
			}
		case strings.HasPrefix(part, "*/"):
			step, err := strconv.Atoi(part[2:])
			if err != nil || step <= 0 {
				return nil, fmt.Errorf("invalid step %q", part)
			}
			for i := min; i <= max; i += step {
				result = append(result, i)
			}
		case strings.Contains(part, "-"):
			vals, err := parseCronRange(part, min, max)
			if err != nil {
				return nil, err
			}
			result = append(result, vals...)
		default:
			val, err := strconv.Atoi(part)
			if err != nil {
				return nil, fmt.Errorf("invalid value %q", part)
			}
			if val < min || val > max {
				return nil, fmt.Errorf("value %d out of bounds [%d, %d]", val, min, max)
			}
			result = append(result, val)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty field %q", field)
	}
	return result, nil
}

// parseCronRange parses an "n-m" cron range bounded by [min, max].
func parseCronRange(part string, min, max int) ([]int, error) {
	bounds := strings.SplitN(part, "-", 2)
	lo, err := strconv.Atoi(bounds[0])
	if err != nil {
		return nil, fmt.Errorf("invalid range start in %q", part)
	}
	hi, err := strconv.Atoi(bounds[1])
	if err != nil {
		return nil, fmt.Errorf("invalid range end in %q", part)
	}
	if lo < min || hi > max || lo > hi {
		return nil, fmt.Errorf("range %q out of bounds [%d, %d]", part, min, max)
	}
	result := make([]int, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		result = append(result, i)
	}
	return result, nil
}
