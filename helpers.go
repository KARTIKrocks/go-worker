package worker

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
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
func NewTicker(pool *Pool, interval time.Duration, job Job) *Ticker {
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
type Debouncer struct {
	pool    *Pool
	delay   time.Duration
	timer   *time.Timer
	mu      sync.Mutex
	pending Job
	leading bool
	fired   bool
}

// NewDebouncer creates a trailing-edge debouncer.
func NewDebouncer(pool *Pool, delay time.Duration) *Debouncer {
	return &Debouncer{pool: pool, delay: delay}
}

// NewDebouncerLeading creates a leading-edge debouncer that fires immediately
// on the first call, then ignores subsequent calls within the delay window.
func NewDebouncerLeading(pool *Pool, delay time.Duration) *Debouncer {
	return &Debouncer{pool: pool, delay: delay, leading: true}
}

// Submit submits a job with debouncing.
func (d *Debouncer) Submit(job Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.leading && !d.fired {
		d.fired = true
		_ = d.pool.SubmitJob(job)
	}

	d.pending = job
	if d.timer != nil {
		d.timer.Stop()
	}

	d.timer = time.AfterFunc(d.delay, func() {
		d.mu.Lock()
		j := d.pending
		d.pending = nil
		d.fired = false
		d.mu.Unlock()

		if j != nil && !d.leading {
			_ = d.pool.SubmitJob(j)
		}
	})
}

// SubmitFunc submits a function with debouncing.
func (d *Debouncer) SubmitFunc(fn func(context.Context) error) {
	d.Submit(JobFunc(fn))
}

// Cancel cancels the pending debounced job.
func (d *Debouncer) Cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.pending = nil
	d.fired = false
}

// Flush immediately executes the pending job, if any.
func (d *Debouncer) Flush() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	if d.pending != nil {
		_ = d.pool.SubmitJob(d.pending)
		d.pending = nil
	}
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
}

// NewThrottler creates a throttler.
func NewThrottler(pool *Pool, interval time.Duration) *Throttler {
	return &Throttler{pool: pool, interval: interval}
}

// Submit submits a job if enough time has passed since the last execution.
// Returns true if the job was submitted.
func (t *Throttler) Submit(job Job) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	if now := time.Now(); now.Sub(t.lastRun) >= t.interval {
		t.lastRun = now
		_ = t.pool.SubmitJob(job)
		return true
	}
	return false
}

// SubmitTrailing submits a job that will fire at the end of the throttle window.
// Repeated calls within the window replace the pending job.
func (t *Throttler) SubmitTrailing(job Job) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(t.lastRun)

	if elapsed >= t.interval {
		t.lastRun = now
		_ = t.pool.SubmitJob(job)
		return
	}

	t.trailing = job
	if t.timer == nil {
		remaining := t.interval - elapsed
		t.timer = time.AfterFunc(remaining, func() {
			t.mu.Lock()
			if t.trailing != nil {
				t.lastRun = time.Now()
				_ = t.pool.SubmitJob(t.trailing)
				t.trailing = nil
			}
			t.timer = nil
			t.mu.Unlock()
		})
	}
}

// Force submits a job unconditionally, resetting the throttle window.
func (t *Throttler) Force(job Job) {
	t.mu.Lock()
	t.lastRun = time.Now()
	t.mu.Unlock()
	_ = t.pool.SubmitJob(job)
}

// Reset clears the throttle state.
func (t *Throttler) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastRun = time.Time{}
	if t.timer != nil {
		t.timer.Stop()
		t.timer = nil
	}
	t.trailing = nil
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
func NewRateLimiter(pool *Pool, n int, interval time.Duration) *RateLimiter {
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
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			}
		}
	}
}

// Submit waits for a token and then submits the job. Respects the provided context.
func (rl *RateLimiter) Submit(ctx context.Context, job Job) error {
	select {
	case <-rl.tokens:
		return rl.pool.SubmitContext(ctx, job)
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.ctx.Done():
		return ErrPoolClosed
	}
}

// TrySubmit attempts to submit without waiting for a token.
func (rl *RateLimiter) TrySubmit(job Job) error {
	select {
	case <-rl.tokens:
		return rl.pool.TrySubmitJob(job)
	default:
		return ErrPoolFull
	}
}

// Stop stops the token refiller.
func (rl *RateLimiter) Stop() {
	rl.cancel()
	rl.wg.Wait()
}
