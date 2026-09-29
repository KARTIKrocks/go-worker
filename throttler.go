package worker

import (
	"sync"
	"time"
)

// Throttler limits job execution to at most once per interval.
type Throttler struct {
	pool     *Pool
	interval time.Duration

	mu       sync.Mutex
	lastRun  time.Time
	trailing Job
	timer    *time.Timer
	gen      uint64 // bumped when a pending trailing job is cancelled; stale callbacks then do nothing
}

// NewThrottler creates a throttler.
func NewThrottler(pool *Pool, interval time.Duration) *Throttler {
	return &Throttler{pool: pool, interval: interval}
}

// Submit submits a job if enough time has passed since the last execution.
// It returns true if the job was submitted successfully.
func (t *Throttler) Submit(job Job) bool {
	if !t.claim() {
		return false
	}
	return t.pool.SubmitJob(job) == nil
}

// claim reports whether the throttle window is open, and if so starts a new one.
func (t *Throttler) claim() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	if now.Sub(t.lastRun) < t.interval {
		return false
	}
	t.openWindow(now)
	return true
}

// openWindow starts a new throttle window. The job starting it supersedes
// any pending trailing job, which would otherwise also run in this window.
// The caller must hold t.mu.
func (t *Throttler) openWindow(now time.Time) {
	t.lastRun = now
	t.cancelTrailing()
}

// cancelTrailing drops the pending trailing job and invalidates its timer,
// including a callback that has already fired and is waiting on t.mu.
// The caller must hold t.mu.
func (t *Throttler) cancelTrailing() {
	t.gen++
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	t.trailing = nil
}

// SubmitTrailing submits a job that will fire at the end of the throttle window.
// Repeated calls within the window replace the pending job. If the window is
// already open, the job is submitted immediately. Submit errors are dropped.
func (t *Throttler) SubmitTrailing(job Job) {
	t.mu.Lock()
	now := time.Now()
	elapsed := now.Sub(t.lastRun)
	if elapsed >= t.interval {
		t.openWindow(now)
		t.mu.Unlock()
		_ = t.pool.SubmitJob(job)
		return
	}

	t.trailing = job
	if t.timer == nil {
		gen := t.gen
		t.timer = time.AfterFunc(t.interval-elapsed, func() { t.fireTrailing(gen) })
	}
	t.mu.Unlock()
}

// fireTrailing submits the trailing job at the end of the window, unless it
// has been cancelled since the timer was created.
func (t *Throttler) fireTrailing(gen uint64) {
	t.mu.Lock()
	if gen != t.gen {
		t.mu.Unlock()
		return
	}
	job := t.trailing
	t.trailing, t.timer = nil, nil
	if job != nil {
		t.lastRun = time.Now()
	}
	t.mu.Unlock()

	if job != nil {
		_ = t.pool.SubmitJob(job)
	}
}

// Force submits a job unconditionally, starting a new throttle window and
// dropping any pending trailing job. It returns the submit error.
func (t *Throttler) Force(job Job) error {
	t.mu.Lock()
	t.openWindow(time.Now())
	t.mu.Unlock()
	return t.pool.SubmitJob(job)
}

// Reset clears the throttle state and drops any pending trailing job.
func (t *Throttler) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastRun = time.Time{}
	t.cancelTrailing()
}
