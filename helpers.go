package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel errors returned by [RateLimiter].
var (
	ErrRateLimited    = errors.New("worker: rate limit exceeded")
	ErrLimiterStopped = errors.New("worker: rate limiter stopped")
)

// Ticker submits a job at a fixed interval.
type Ticker struct {
	pool     *Pool
	job      Job
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	runFirst bool
	started  atomic.Bool
}

// NewTicker creates a ticker that submits job every interval.
// It panics if interval <= 0.
func NewTicker(pool *Pool, interval time.Duration, job Job) *Ticker {
	if interval <= 0 {
		panic(fmt.Sprintf("worker: NewTicker: interval must be > 0, got %v", interval))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Ticker{pool: pool, job: job, interval: interval, ctx: ctx, cancel: cancel}
}

// NewTickerFunc creates a ticker from a function.
func NewTickerFunc(pool *Pool, interval time.Duration, fn func(context.Context) error) *Ticker {
	return NewTicker(pool, interval, JobFunc(fn))
}

// NewTickerImmediate creates a ticker that fires immediately, then at interval.
func NewTickerImmediate(pool *Pool, interval time.Duration, job Job) *Ticker {
	t := NewTicker(pool, interval, job)
	t.runFirst = true
	return t
}

// Start begins the ticker loop. It is idempotent.
func (t *Ticker) Start() {
	if t.started.Swap(true) {
		return
	}
	t.wg.Add(1)
	go t.run()
}

func (t *Ticker) run() {
	defer t.wg.Done()

	if t.runFirst {
		t.submit()
	}

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.submit()
		}
	}
}

// submit enqueues the job. Waiting for queue space is bounded by the ticker's
// context so Stop is never blocked behind a full or paused pool, but the job
// itself runs detached from it: stopping the ticker does not cancel runs
// that were already submitted.
func (t *Ticker) submit() {
	_ = t.pool.submit(t.ctx, context.Background(), t.job, nil, true)
}

// Stop stops the ticker and waits for the goroutine to exit. Runs already
// submitted to the pool are not cancelled.
func (t *Ticker) Stop() {
	t.cancel()
	t.wg.Wait()
}

// Debouncer collapses rapid submissions, executing only the last job
// after a quiet period of the configured delay.
//
// Jobs submitted when a timer fires are submitted with [Pool.SubmitJob];
// if that fails (for example, the pool is closed) the job is dropped.
type Debouncer struct {
	pool    *Pool
	delay   time.Duration
	leading bool

	mu      sync.Mutex
	timer   *time.Timer
	gen     uint64 // bumped by Submit, Cancel and Flush; stale timer callbacks see a newer value and do nothing
	pending Job    // trailing mode only
	fired   bool   // leading mode: the current window has already fired
}

// NewDebouncer creates a trailing-edge debouncer.
func NewDebouncer(pool *Pool, delay time.Duration) *Debouncer {
	return &Debouncer{pool: pool, delay: delay}
}

// NewDebouncerLeading creates a leading-edge debouncer that fires immediately
// on the first call, then ignores subsequent calls until delay has passed
// without a call.
func NewDebouncerLeading(pool *Pool, delay time.Duration) *Debouncer {
	return &Debouncer{pool: pool, delay: delay, leading: true}
}

// Submit submits a job with debouncing.
func (d *Debouncer) Submit(job Job) {
	d.mu.Lock()
	var now Job
	if d.leading {
		if !d.fired {
			d.fired = true
			now = job
		}
	} else {
		d.pending = job
	}
	d.gen++
	gen := d.gen
	if d.timer != nil {
		d.timer.Stop()
	}
	d.timer = time.AfterFunc(d.delay, func() { d.fire(gen) })
	d.mu.Unlock()

	if now != nil {
		_ = d.pool.SubmitJob(now)
	}
}

// fire ends the quiet period started by the Submit that produced gen.
// Stopping a timer does not stop a callback that has already started, so a
// callback from a superseded Submit, or one racing Cancel or Flush, must
// do nothing.
func (d *Debouncer) fire(gen uint64) {
	d.mu.Lock()
	if gen != d.gen {
		d.mu.Unlock()
		return
	}
	job := d.pending
	d.pending, d.fired, d.timer = nil, false, nil
	d.mu.Unlock()

	if job != nil {
		_ = d.pool.SubmitJob(job)
	}
}

// SubmitFunc submits a function with debouncing.
func (d *Debouncer) SubmitFunc(fn func(context.Context) error) {
	d.Submit(JobFunc(fn))
}

// Cancel cancels the pending debounced job.
func (d *Debouncer) Cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reset()
}

// Flush immediately submits the pending job, if any, and returns the submit
// error. In leading mode there is never a pending job; Flush only ends the
// current window.
func (d *Debouncer) Flush() error {
	d.mu.Lock()
	job := d.pending
	d.reset()
	d.mu.Unlock()

	if job == nil {
		return nil
	}
	return d.pool.SubmitJob(job)
}

// reset stops the timer and clears all state. The caller must hold d.mu.
func (d *Debouncer) reset() {
	d.gen++
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.pending = nil
	d.fired = false
}

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

// RateLimiter is a token-bucket rate limiter that wraps a worker pool.
type RateLimiter struct {
	pool   *Pool
	tokens chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRateLimiter creates a rate limiter that allows n jobs per interval.
// It panics if n < 1 or interval <= 0.
func NewRateLimiter(pool *Pool, n int, interval time.Duration) *RateLimiter {
	if n < 1 || interval <= 0 {
		panic(fmt.Sprintf("worker: NewRateLimiter: need n >= 1 and interval > 0, got n=%d interval=%v", n, interval))
	}
	ctx, cancel := context.WithCancel(context.Background())
	rl := &RateLimiter{
		pool:   pool,
		tokens: make(chan struct{}, n),
		ctx:    ctx,
		cancel: cancel,
	}
	for range n {
		rl.tokens <- struct{}{}
	}
	rl.wg.Add(1)
	go rl.refill(n, interval)
	return rl
}

func (rl *RateLimiter) refill(n int, interval time.Duration) {
	defer rl.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-rl.ctx.Done():
			return
		case <-ticker.C:
			for range n {
				rl.putToken()
			}
		}
	}
}

// putToken returns a token to the bucket unless it is already full.
func (rl *RateLimiter) putToken() {
	select {
	case rl.tokens <- struct{}{}:
	default:
	}
}

// Submit waits for a token and then submits the job. Respects the provided
// context. Returns [ErrLimiterStopped] once [RateLimiter.Stop] has been called.
// If the pool rejects the job, the token is returned.
func (rl *RateLimiter) Submit(ctx context.Context, job Job) error {
	if rl.ctx.Err() != nil {
		return ErrLimiterStopped
	}
	select {
	case <-rl.tokens:
		if err := rl.pool.SubmitContext(ctx, job); err != nil {
			rl.putToken()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.ctx.Done():
		return ErrLimiterStopped
	}
}

// TrySubmit attempts to submit without waiting for a token. It returns
// [ErrRateLimited] if no token is available, or the pool's error (such as
// [ErrPoolFull]) if the pool rejects the job, in which case the token is returned.
func (rl *RateLimiter) TrySubmit(job Job) error {
	if rl.ctx.Err() != nil {
		return ErrLimiterStopped
	}
	select {
	case <-rl.tokens:
		if err := rl.pool.TrySubmitJob(job); err != nil {
			rl.putToken()
			return err
		}
		return nil
	default:
		return ErrRateLimited
	}
}

// Stop stops the token refiller. Subsequent submits return [ErrLimiterStopped].
func (rl *RateLimiter) Stop() {
	rl.cancel()
	rl.wg.Wait()
}
