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
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	submitMu sync.Mutex // guards concurrent submit vs close(p.jobs)

	closed  atomic.Bool
	paused  atomic.Bool
	pauseMu sync.Mutex
	pauseCh chan struct{} // closed when unpaused; recreated on pause

	metrics    Metrics
	logger     Logger
	hooks      Hooks
	middleware []Middleware
}

// jobEnvelope wraps a job with its submission context and an optional result channel.
type jobEnvelope struct {
	job      Job
	ctx      context.Context
	resultCh chan<- error // nil for fire-and-forget
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
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}

	p.cfg = cfg
	p.jobs = make(chan jobEnvelope, cfg.QueueSize)
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
	return p.submitCtx(context.Background(), job, nil)
}

// SubmitWait submits a function and blocks until it completes, returning its error.
func (p *Pool) SubmitWait(ctx context.Context, fn func(ctx context.Context) error) error {
	return p.SubmitJobWait(ctx, JobFunc(fn))
}

// SubmitJobWait submits a [Job] and blocks until it completes.
func (p *Pool) SubmitJobWait(ctx context.Context, job Job) error {
	ch := make(chan error, 1)
	if err := p.submitCtx(ctx, job, ch); err != nil {
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
	p.submitMu.Lock()
	defer p.submitMu.Unlock()
	if p.closed.Load() {
		return ErrPoolClosed
	}
	select {
	case p.jobs <- jobEnvelope{job: job, ctx: context.Background()}:
		p.metrics.JobsSubmitted.Add(1)
		p.metrics.QueueLength.Add(1)
		return nil
	default:
		return ErrPoolFull
	}
}

// SubmitContext submits a [Job] with a context. If the context expires before
// the job can be enqueued, the context error is returned. If the queue is full,
// it blocks until space is available, the context expires, or the pool closes.
func (p *Pool) SubmitContext(ctx context.Context, job Job) error {
	return p.submitCtx(ctx, job, nil)
}

// submitCtx is the core submit path.
func (p *Pool) submitCtx(ctx context.Context, job Job, resultCh chan<- error) error {
	p.submitMu.Lock()
	if p.closed.Load() {
		p.submitMu.Unlock()
		return ErrPoolClosed
	}

	envelope := jobEnvelope{
		job:      job,
		ctx:      ctx,
		resultCh: resultCh,
	}

	// Hold submitMu only for the non-blocking attempt to prevent send-on-closed-channel.
	select {
	case p.jobs <- envelope:
		p.submitMu.Unlock()
		p.metrics.JobsSubmitted.Add(1)
		p.metrics.QueueLength.Add(1)
		return nil
	default:
	}
	p.submitMu.Unlock()

	// Blocking path: queue was full, wait for space or cancellation.
	select {
	case p.jobs <- envelope:
		p.metrics.JobsSubmitted.Add(1)
		p.metrics.QueueLength.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return ErrPoolClosed
	}
}

// Pause pauses all workers. Workers finish their current job but do not
// pick up new ones until [Pool.Resume] is called.
func (p *Pool) Pause() {
	p.pauseMu.Lock()
	defer p.pauseMu.Unlock()
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
	return p.metrics.Snapshot()
}

// QueueLength returns the current number of jobs in the queue.
func (p *Pool) QueueLength() int {
	return int(p.metrics.QueueLength.Load())
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
// don't finish in time, remaining jobs are cancelled via context.
func (p *Pool) CloseWithTimeout(timeout time.Duration) error {
	if p.closed.Swap(true) {
		return nil // already closed
	}

	if p.logger != nil {
		p.logger.Info("pool shutting down")
	}

	// Resume if paused so workers can drain.
	p.Resume()

	// Close the jobs channel under submitMu to prevent send-on-closed-channel
	// races with concurrent submitters. Setting closed=true above ensures new
	// submitters see ErrPoolClosed before touching the channel.
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
		// Block while paused. pauseCh is closed when not paused.
		p.pauseMu.Lock()
		ch := p.pauseCh
		p.pauseMu.Unlock()
		<-ch

		select {
		case <-p.ctx.Done():
			return
		case envelope, ok := <-p.jobs:
			if !ok {
				return
			}
			p.metrics.QueueLength.Add(-1)
			p.metrics.ActiveWorkers.Add(1)

			err := p.processJob(envelope)

			p.metrics.ActiveWorkers.Add(-1)

			if envelope.resultCh != nil {
				envelope.resultCh <- err
				close(envelope.resultCh)
			}
		}
	}
}

// processJob processes a job with timeout, retries, middleware, and panic recovery.
func (p *Pool) processJob(env jobEnvelope) (finalErr error) {
	start := time.Now()
	job := env.job

	// Derive job context from the pool's context so that pool cancellation
	// (e.g. CloseWithTimeout) reaches in-flight jobs.
	ctx, jobCancel := context.WithCancel(env.ctx)
	defer jobCancel()

	if p.hooks.OnJobStart != nil {
		p.hooks.OnJobStart(job)
	}

	// Build the processing function with middleware chain
	run := func(ctx context.Context) error {
		return job.Process(ctx)
	}
	for i := len(p.middleware) - 1; i >= 0; i-- {
		run = p.middleware[i](run)
	}

	// Apply job timeout
	if p.cfg.JobTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.cfg.JobTimeout)
		defer cancel()
	}

	// Start pool-context watcher AFTER ctx is finalized to avoid a race
	// on the ctx variable.
	poolDone := p.ctx.Done()
	ctxDone := ctx.Done()
	go func() {
		select {
		case <-poolDone:
			jobCancel()
		case <-ctxDone:
		}
	}()

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

			delay := p.cfg.RetryDelay
			if p.cfg.RetryBackoff {
				delay *= 1 << min(attempt-1, 30)
			}
			// Cap delay at MaxRetryDelay if configured.
			if p.cfg.MaxRetryDelay > 0 && delay > p.cfg.MaxRetryDelay {
				delay = p.cfg.MaxRetryDelay
			}

			if p.logger != nil {
				p.logger.Warn("job failed, retrying",
					"attempt", attempt, "max", maxAttempts,
					"delay", delay, "error", finalErr)
			}

			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				finalErr = ctx.Err()
				return
			case <-timer.C:
			}
		}
	}

	duration := time.Since(start)
	p.metrics.TotalDuration.Add(int64(duration))

	if finalErr != nil {
		p.metrics.JobsFailed.Add(1)
		if p.hooks.OnJobFailed != nil {
			p.hooks.OnJobFailed(job, finalErr, duration)
		}
	} else {
		p.metrics.JobsCompleted.Add(1)
		if p.hooks.OnJobComplete != nil {
			p.hooks.OnJobComplete(job, duration)
		}
	}

	return finalErr
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
