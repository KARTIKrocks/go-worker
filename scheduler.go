package worker

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime/debug"
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

// IntervalSchedule fires at fixed intervals. A non-positive Interval never fires.
type IntervalSchedule struct {
	Interval time.Duration
}

// Next implements [Schedule].
func (s *IntervalSchedule) Next(after time.Time) time.Time {
	if s.Interval <= 0 {
		return time.Time{}
	}
	return after.Add(s.Interval)
}

// FixedTimeSchedule fires at a start time, then repeats at a fixed interval.
// A non-positive Interval fires once at Start.
type FixedTimeSchedule struct {
	Start    time.Time
	Interval time.Duration
}

// Next implements [Schedule].
func (s *FixedTimeSchedule) Next(after time.Time) time.Time {
	if after.Before(s.Start) {
		return s.Start
	}
	if s.Interval <= 0 {
		return time.Time{}
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

// DailySchedule fires at specific HH:MM wall-clock times each day, in the
// time zone of the time passed to Next. A time skipped by a daylight-saving
// change (02:30 when clocks jump from 02:00 to 03:00) fires after the jump
// (03:30).
type DailySchedule struct {
	times []time.Duration // time of day as hours+minutes, sorted ascending
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
	// Build each candidate from the calendar date and clock time rather than
	// adding a duration to midnight: on daylight-saving change days a day is
	// 23 or 25 hours long, and midnight.Add(9h) would not be 09:00.
	for day := range 3 {
		for _, t := range s.times {
			h, m := int(t/time.Hour), int(t%time.Hour/time.Minute)
			c := time.Date(after.Year(), after.Month(), after.Day()+day, h, m, 0, 0, after.Location())
			// A time inside a spring-forward gap does not exist, and time.Date
			// moves it backwards (02:30 becomes 01:30 EST). Shift it forward
			// by the gap instead (02:30 becomes 03:30 EDT), so it still runs
			// that day, after the clock change.
			if diff := (h*60 + m) - (c.Hour()*60 + c.Minute()); diff != 0 {
				if diff < 0 {
					diff += 24 * 60 // normalized back across midnight
				}
				c = c.Add(time.Duration(diff) * time.Minute)
			}
			if c.After(after) {
				return c
			}
		}
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
	lifeMu  sync.Mutex // serializes Start and Stop

	tickInterval time.Duration
	onTaskStart  func(name string)
	onTaskEnd    func(name string, err error, duration time.Duration)
}

type scheduledTask struct {
	name     string
	job      Job
	schedule Schedule
	running  atomic.Int32
	overlap  OverlapPolicy
	queued   atomic.Int32

	// ctx is cancelled when the task is removed, replaced, or the scheduler
	// stops. All runs of the task share it.
	ctx    context.Context
	cancel context.CancelFunc

	mu   sync.Mutex // protects next and the stats fields
	next time.Time

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
// For a [OnceSchedule] task, the immediate run replaces the scheduled one.
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

// Schedule adds a job with a custom [Schedule]. Scheduling a name that is
// already registered replaces that task and cancels its in-flight runs.
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
		// A once task must not run again at its scheduled time.
		if once, ok := sched.(*OnceSchedule); ok {
			once.done.Store(true)
		}
	}
	task.ctx, task.cancel = context.WithCancel(s.ctx)

	s.tasksMu.Lock()
	if old, ok := s.tasks[name]; ok {
		old.cancel()
	}
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
		t.cancel()
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

// Trigger runs a task now, outside its normal schedule. Like a scheduled run,
// it respects the task's pause state, [WithMaxRuns] limit and overlap policy.
// It reports whether a run was started or queued; false also means the task
// does not exist.
func (s *Scheduler) Trigger(name string) bool {
	s.tasksMu.RLock()
	t, ok := s.tasks[name]
	s.tasksMu.RUnlock()
	if !ok {
		return false
	}
	return s.dispatch(t)
}

// Start begins the scheduler loop. It is idempotent. A stopped scheduler
// cannot be restarted: Start after [Scheduler.Stop] does nothing.
func (s *Scheduler) Start() {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	if s.running.Load() || s.ctx.Err() != nil {
		return
	}
	s.running.Store(true)
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
		// Paused or exhausted tasks keep their due slot, so a resumed task
		// runs once promptly instead of waiting a full period.
		if task.paused.Load() || task.maxRunsReached() {
			continue
		}

		task.mu.Lock()
		if task.next.IsZero() || now.Before(task.next) {
			task.mu.Unlock()
			continue
		}
		// This slot is consumed whether the task runs, is skipped, or is queued.
		task.next = task.nextRun(now)
		task.mu.Unlock()

		s.dispatch(task)
	}
}

// dispatch starts a run of task now, subject to its pause state, WithMaxRuns
// limit and overlap policy. It reports whether a run was started or queued.
// Both the scheduler loop and Trigger go through here.
func (s *Scheduler) dispatch(task *scheduledTask) bool {
	if task.paused.Load() || task.ctx.Err() != nil {
		return false
	}

	// Claim the running slot atomically so two dispatches (a tick and a
	// Trigger) cannot both start a run under OverlapSkip or OverlapQueue.
	if task.overlap == OverlapAllow {
		task.running.Add(1)
	} else if !task.running.CompareAndSwap(0, 1) {
		return task.overlap == OverlapQueue && task.enqueue()
	}

	if !task.claimRun() {
		task.running.Add(-1)
		return false
	}
	go s.executeTask(task)
	return true
}

// enqueue queues one run behind the in-flight one, unless the queue is full
// or the queued runs would already use up the WithMaxRuns limit.
func (t *scheduledTask) enqueue() bool {
	for {
		q := t.queued.Load()
		if q >= maxQueuedPerTask || (t.maxRuns > 0 && t.runCnt.Load()+int64(q) >= int64(t.maxRuns)) {
			return false
		}
		if t.queued.CompareAndSwap(q, q+1) {
			return true
		}
	}
}

// maxRunsReached reports whether the task has used up its WithMaxRuns limit.
func (t *scheduledTask) maxRunsReached() bool {
	return t.maxRuns > 0 && t.runCnt.Load() >= int64(t.maxRuns)
}

// claimRun counts one run against the WithMaxRuns limit, failing if the
// limit is already reached.
func (t *scheduledTask) claimRun() bool {
	for {
		n := t.runCnt.Load()
		if t.maxRuns > 0 && n >= int64(t.maxRuns) {
			return false
		}
		if t.runCnt.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// nextRun returns the task's next run time after now, with jitter applied.
// The caller must hold t.mu.
func (t *scheduledTask) nextRun(now time.Time) time.Time {
	next := t.schedule.Next(now)
	if t.jitter > 0 && !next.IsZero() {
		next = next.Add(time.Duration(rand.Int64N(int64(t.jitter)))) //nolint:gosec // scheduling jitter is not security-sensitive
	}
	return next
}

// executeTask performs one run of task. The caller has already claimed the
// run (running and runCnt are incremented).
func (s *Scheduler) executeTask(task *scheduledTask) {
	defer s.finishRun(task)

	if s.logger != nil {
		s.logger.Debug("running task", "name", task.name)
	}
	if s.onTaskStart != nil {
		s.callHook("OnTaskStart", func() { s.onTaskStart(task.name) })
	}

	start := time.Now()
	err := s.pool.SubmitJobWait(task.ctx, task.job)
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
		s.callHook("OnTaskEnd", func() { s.onTaskEnd(task.name, err, dur) })
	}
}

// finishRun releases a finished run's slot, handing it straight to a queued
// run if there is one, so no other dispatch can start in between.
func (s *Scheduler) finishRun(task *scheduledTask) {
	for {
		if task.queued.Load() > 0 {
			if task.ctx.Err() == nil && task.claimRun() {
				task.queued.Add(-1)
				go s.executeTask(task) // the queued run inherits this slot
				return
			}
			task.queued.Store(0)
		}
		task.running.Add(-1)

		// A dispatch that saw the slot still taken may have queued a run
		// between the check above and the decrement. If so, and nobody else
		// has taken the slot, take it back and hand it over; otherwise the
		// current holder will see the queued run when it finishes.
		if task.queued.Load() == 0 || !task.running.CompareAndSwap(0, 1) {
			return
		}
	}
}

// callHook runs a user callback, recovering a panic so a faulty callback
// cannot crash the process. The panic is logged if a logger is set; a panic
// from the logger itself is dropped.
func (s *Scheduler) callHook(name string, fn func()) {
	defer func() {
		if r := recover(); r != nil && s.logger != nil {
			defer func() { _ = recover() }()
			s.logger.Error("scheduler hook panicked", "hook", name, "recovered", r, "stack", string(debug.Stack()))
		}
	}()
	fn()
}

// Stop stops the scheduler and waits for the loop goroutine to exit. It is
// permanent: the scheduler cannot be started again. The contexts of in-flight
// task runs are cancelled, but Stop does not wait for those runs to return.
func (s *Scheduler) Stop() {
	s.lifeMu.Lock()
	defer s.lifeMu.Unlock()
	s.cancel() // also makes a never-started scheduler permanently stopped
	if !s.running.Swap(false) {
		return
	}
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
