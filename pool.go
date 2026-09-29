// Package worker provides a high-performance worker pool for concurrent job
// processing with retries, panic recovery, middleware, and observability.
//
// Basic usage:
//
//	pool, err := worker.NewPool(
//	    worker.WithWorkers(8),
//	    worker.WithQueueSize(500),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer pool.Close()
//
//	pool.Submit(func(ctx context.Context) error {
//	    return doWork(ctx)
//	})
package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel errors returned by Pool operations.
var (
	ErrPoolClosed    = errors.New("worker: pool is closed")
	ErrPoolFull      = errors.New("worker: queue is full")
	ErrJobPanic      = errors.New("worker: job panicked")
	ErrInvalidConfig = errors.New("worker: invalid configuration")
)

// Job represents a unit of work. Implement this interface for complex jobs
// that need to carry state. For simple functions, use [Pool.Submit] directly.
type Job interface {
	Process(ctx context.Context) error
}

// JobFunc adapts a function to the [Job] interface.
type JobFunc func(ctx context.Context) error

// Process implements [Job].
func (f JobFunc) Process(ctx context.Context) error {
	return f(ctx)
}

// Pool manages a set of worker goroutines that process jobs concurrently.
// It is safe for concurrent use by multiple goroutines.
type Pool struct {
	cfg Config

	jobs     chan jobEnvelope
	quit     chan struct{} // closed when Close begins; unblocks waiting submitters
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	submitMu sync.RWMutex // submitters hold RLock while sending; Close takes Lock to close(p.jobs)

	closed  atomic.Bool
	paused  atomic.Bool
	pauseMu sync.Mutex
	pauseCh chan struct{} // closed when unpaused; recreated on pause

	metrics    Metrics
	logger     Logger
	hooks      Hooks
	middleware []Middleware
}

// jobEnvelope wraps a job with its submission context and an optional
// completion callback.
type jobEnvelope struct {
	job  Job
	ctx  context.Context
	done func(err error) // nil for fire-and-forget; called exactly once with the final result
}

// NewPool creates and starts a new worker pool.
// Workers begin processing immediately. Use [Pool.Close] for graceful shutdown.
func NewPool(opts ...Option) (*Pool, error) {
	cfg := Config{
		Workers:      4,
		QueueSize:    100,
		JobTimeout:   30 * time.Second,
		MaxRetries:   0,
		RetryDelay:   time.Second,
		RetryBackoff: true,
	}

	p := &Pool{
		pauseCh: make(chan struct{}),
	}
	// unpause by default
	close(p.pauseCh)

	for _, opt := range opts {
		opt(p, &cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidConfig, err)
	}

	p.cfg = cfg
	p.jobs = make(chan jobEnvelope, cfg.QueueSize)
	p.quit = make(chan struct{})
	p.ctx, p.cancel = context.WithCancel(context.Background())
	p.metrics.startTime = time.Now()

	for i := range cfg.Workers {
		p.wg.Add(1)
		go p.worker(i)
	}

	if p.logger != nil {
		p.logger.Info("pool started", "workers", cfg.Workers, "queue_size", cfg.QueueSize)
	}

	return p, nil
}

// Submit submits a function for processing. It blocks if the queue is full
// until space is available or the pool is closed.
func (p *Pool) Submit(fn func(ctx context.Context) error) error {
	return p.SubmitJob(JobFunc(fn))
}

// SubmitJob submits a [Job] for processing. It blocks if the queue is full.
func (p *Pool) SubmitJob(job Job) error {
	return p.submit(context.Background(), context.Background(), job, nil, true)
}

// SubmitWait submits a function and blocks until it completes, returning its error.
func (p *Pool) SubmitWait(ctx context.Context, fn func(ctx context.Context) error) error {
	return p.SubmitJobWait(ctx, JobFunc(fn))
}

// SubmitJobWait submits a [Job] and blocks until it completes.
// The job runs with a context derived from ctx, so cancelling ctx both stops
// the wait (returning ctx.Err()) and cancels the job.
func (p *Pool) SubmitJobWait(ctx context.Context, job Job) error {
	ch := make(chan error, 1)
	if err := p.submit(ctx, ctx, job, func(err error) { ch <- err }, true); err != nil {
		return err
	}
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TrySubmit attempts to submit a function without blocking.
// Returns [ErrPoolFull] if the queue is full.
func (p *Pool) TrySubmit(fn func(ctx context.Context) error) error {
	return p.TrySubmitJob(JobFunc(fn))
}

// TrySubmitJob attempts to submit a [Job] without blocking.
func (p *Pool) TrySubmitJob(job Job) error {
	return p.submit(context.Background(), context.Background(), job, nil, false)
}

// SubmitContext submits a [Job] with a context. If the context expires before
// the job can be enqueued, the context error is returned. If the queue is full,
// it blocks until space is available, the context expires, or the pool closes.
func (p *Pool) SubmitContext(ctx context.Context, job Job) error {
	return p.submit(ctx, ctx, job, nil, true)
}

// submit is the core submit path. waitCtx bounds how long a blocking submit
// waits for queue space; jobCtx is the parent of the context the job runs
// with. If done is non-nil and submit returns nil, done is called exactly once
// with the job's final result (or [ErrPoolClosed] if the job is dropped during
// a timed-out shutdown).
func (p *Pool) submit(waitCtx, jobCtx context.Context, job Job, done func(error), block bool) error {
	// Holding the read lock for the whole send guarantees Close cannot close
	// p.jobs underneath us. Close unblocks waiting senders via p.quit first.
	p.submitMu.RLock()
	defer p.submitMu.RUnlock()
	if p.closed.Load() {
		return ErrPoolClosed
	}

	envelope := jobEnvelope{job: job, ctx: jobCtx, done: done}

	if !block {
		select {
		case p.jobs <- envelope:
			p.metrics.JobsSubmitted.Add(1)
			return nil
		default:
			return ErrPoolFull
		}
	}

	select {
	case p.jobs <- envelope:
		p.metrics.JobsSubmitted.Add(1)
		return nil
	case <-waitCtx.Done():
		return waitCtx.Err()
	case <-p.quit:
		return ErrPoolClosed
	}
}

// Pause pauses all workers. Workers finish their current job but do not
// pick up new ones until [Pool.Resume] is called.
func (p *Pool) Pause() {
	p.pauseMu.Lock()
	defer p.pauseMu.Unlock()
	// A closing pool must be able to drain, so Pause is a no-op once Close
	// has started. Close sets closed before calling Resume, so this check
	// under pauseMu cannot miss it.
	if p.closed.Load() {
		return
	}
	if !p.paused.Load() {
		p.pauseCh = make(chan struct{})
		p.paused.Store(true)
		if p.logger != nil {
			p.logger.Info("pool paused")
		}
	}
}

// Resume resumes a paused pool.
func (p *Pool) Resume() {
	p.pauseMu.Lock()
	defer p.pauseMu.Unlock()
	if p.paused.Load() {
		p.paused.Store(false)
		close(p.pauseCh)
		if p.logger != nil {
			p.logger.Info("pool resumed")
		}
	}
}

// IsPaused reports whether the pool is paused.
func (p *Pool) IsPaused() bool {
	return p.paused.Load()
}

// IsClosed reports whether the pool has been closed.
func (p *Pool) IsClosed() bool {
	return p.closed.Load()
}

// Snapshot returns a point-in-time copy of pool metrics.
func (p *Pool) Snapshot() MetricsSnapshot {
	s := p.metrics.Snapshot()
	s.QueueLength = int32(p.QueueLength()) //nolint:gosec // bounded by QueueSize
	return s
}

// QueueLength returns the current number of jobs in the queue. Submitters
// blocked waiting for queue space are not counted.
func (p *Pool) QueueLength() int {
	return len(p.jobs)
}

// ActiveWorkers returns the number of workers currently processing a job.
func (p *Pool) ActiveWorkers() int {
	return int(p.metrics.ActiveWorkers.Load())
}

// Close gracefully shuts down the pool. It closes the job queue and waits for
// all in-flight jobs to complete. Close is idempotent.
func (p *Pool) Close() error {
	return p.CloseWithTimeout(0)
}

// CloseWithTimeout gracefully shuts down the pool. If timeout > 0 and workers
// don't finish in time, in-flight jobs are cancelled via context and jobs still
// in the queue are dropped; waiters on dropped jobs receive [ErrPoolClosed].
func (p *Pool) CloseWithTimeout(timeout time.Duration) error {
	if p.closed.Swap(true) {
		return nil // already closed
	}

	if p.logger != nil {
		p.logger.Info("pool shutting down")
	}

	// Unblock submitters waiting on a full queue so they release submitMu.
	close(p.quit)

	// Resume if paused so workers can drain.
	p.Resume()

	// Close the jobs channel under the write lock: no submitter can be
	// mid-send, and setting closed=true above makes new ones bail out.
	p.submitMu.Lock()
	close(p.jobs)
	p.submitMu.Unlock()

	if timeout > 0 {
		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()
		select {
		case <-done:
			// Workers drained cleanly.
		case <-time.After(timeout):
			// Timeout: force-cancel in-flight jobs and wait.
			p.cancel()
			p.wg.Wait()
		}
	} else {
		// No timeout: wait for all workers to drain.
		p.wg.Wait()
	}

	p.cancel()

	// Workers exit early on cancellation, leaving jobs in the queue. Fail
	// them so nobody waits forever on a job that will never run.
	for env := range p.jobs {
		if env.done != nil {
			env.done(ErrPoolClosed)
		}
	}

	if p.logger != nil {
		p.logger.Info("pool stopped")
	}
	return nil
}

// worker is the main goroutine loop for a single worker.
func (p *Pool) worker(id int) {
	defer p.wg.Done()

	if p.hooks.OnWorkerStart != nil {
		p.hooks.OnWorkerStart(id)
	}
	defer func() {
		if p.hooks.OnWorkerStop != nil {
			p.hooks.OnWorkerStop(id)
		}
	}()

	for {
		if !p.waitUnpaused() {
			return
		}

		select {
		case <-p.ctx.Done():
			return
		case envelope, ok := <-p.jobs:
			if !ok {
				return
			}
			// select picks randomly among ready cases, so a force-cancelled
			// pool can still hand us a job; drop it rather than run it.
			if p.ctx.Err() != nil {
				if envelope.done != nil {
					envelope.done(ErrPoolClosed)
				}
				return
			}
			p.metrics.ActiveWorkers.Add(1)

			err := p.processJob(envelope)

			p.metrics.ActiveWorkers.Add(-1)

			if envelope.done != nil {
				envelope.done(err)
			}
		}
	}
}

// waitUnpaused blocks while the pool is paused. It returns false if the pool
// is force-cancelled while waiting.
func (p *Pool) waitUnpaused() bool {
	p.pauseMu.Lock()
	ch := p.pauseCh
	p.pauseMu.Unlock()
	select {
	case <-ch:
		return true
	case <-p.ctx.Done():
		return false
	}
}

// processJob processes a job with timeout, retries, middleware, and panic recovery.
func (p *Pool) processJob(env jobEnvelope) (finalErr error) {
	start := time.Now()
	job := env.job

	// Derive the job context from the submitter's context, and also cancel it
	// when the pool is force-cancelled (e.g. CloseWithTimeout).
	ctx, jobCancel := context.WithCancel(env.ctx)
	defer jobCancel()
	stop := context.AfterFunc(p.ctx, jobCancel)
	defer stop()

	if p.hooks.OnJobStart != nil {
		p.hooks.OnJobStart(job)
	}

	// Build the processing function with middleware chain
	run := func(ctx context.Context) error {
		return job.Process(ctx)
	}
	for _, v := range slices.Backward(p.middleware) {
		run = v(run)
	}

	// Apply job timeout
	if p.cfg.JobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.JobTimeout)
		defer cancel()
	}

	maxAttempts := p.cfg.MaxRetries + 1

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		finalErr = p.safeRun(run, ctx, job)

		if finalErr == nil {
			break
		}

		// Don't retry on context cancellation
		if ctx.Err() != nil {
			finalErr = ctx.Err()
			break
		}

		if attempt < maxAttempts {
			p.metrics.JobsRetried.Add(1)

			delay := p.retryDelay(attempt)

			if p.logger != nil {
				p.logger.Warn("job failed, retrying",
					"attempt", attempt, "max", maxAttempts,
					"delay", delay, "error", finalErr)
			}

			if err := sleepCtx(ctx, delay); err != nil {
				finalErr = err
				break
			}
		}
	}

	p.recordResult(job, finalErr, time.Since(start))
	return finalErr
}

// sleepCtx waits for d or until ctx is done, returning ctx.Err() in the latter case.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// retryDelay returns the delay before the retry following the given attempt,
// applying exponential backoff and the MaxRetryDelay cap when configured.
func (p *Pool) retryDelay(attempt int) time.Duration {
	delay := p.cfg.RetryDelay
	if p.cfg.RetryBackoff {
		delay *= 1 << min(attempt-1, 30)
	}
	if p.cfg.MaxRetryDelay > 0 && delay > p.cfg.MaxRetryDelay {
		delay = p.cfg.MaxRetryDelay
	}
	return delay
}

// recordResult updates metrics and fires the completion hooks for a finished job.
func (p *Pool) recordResult(job Job, err error, duration time.Duration) {
	p.metrics.TotalDuration.Add(int64(duration))

	if err != nil {
		p.metrics.JobsFailed.Add(1)
		if p.hooks.OnJobFailed != nil {
			p.hooks.OnJobFailed(job, err, duration)
		}
		return
	}
	p.metrics.JobsCompleted.Add(1)
	if p.hooks.OnJobComplete != nil {
		p.hooks.OnJobComplete(job, duration)
	}
}

// safeRun runs the function with panic recovery.
func (p *Pool) safeRun(fn func(context.Context) error, ctx context.Context, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: %v", ErrJobPanic, r)
			p.metrics.JobsPanicked.Add(1)
			if p.cfg.PanicHandler != nil {
				p.cfg.PanicHandler(job, r)
			}
			if p.logger != nil {
				p.logger.Error("job panicked", "recovered", r)
			}
		}
	}()
	return fn(ctx)
}
