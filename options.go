package worker

import (
	"context"
	"fmt"
	"time"
)

// config holds worker pool configuration, set through [Option] values.
type config struct {
	Workers       int
	QueueSize     int
	JobTimeout    time.Duration
	MaxRetries    int
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration // 0 means no cap
	RetryBackoff  bool
	RetryJitter   float64 // fraction of each retry delay to randomize, in [0, 1]
	PanicHandler  func(job Job, recovered any)
}

func (c *config) validate() error {
	if c.Workers < 1 {
		return fmt.Errorf("workers must be >= 1, got %d", c.Workers)
	}
	if c.QueueSize < 0 {
		return fmt.Errorf("queue size must be >= 0, got %d", c.QueueSize)
	}
	if c.JobTimeout < 0 {
		return fmt.Errorf("job timeout must be >= 0, got %v", c.JobTimeout)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max retries must be >= 0, got %d", c.MaxRetries)
	}
	if c.RetryDelay < 0 {
		return fmt.Errorf("retry delay must be >= 0, got %v", c.RetryDelay)
	}
	if c.MaxRetryDelay < 0 {
		return fmt.Errorf("max retry delay must be >= 0, got %v", c.MaxRetryDelay)
	}
	if !(c.RetryJitter >= 0 && c.RetryJitter <= 1) { // written this way to also reject NaN
		return fmt.Errorf("retry jitter must be in [0, 1], got %v", c.RetryJitter)
	}
	return nil
}

// Option configures a [Pool]. Pass to [NewPool]; create one with the With...
// functions.
type Option func(p *Pool)

// WithWorkers sets the number of worker goroutines. Default: 4.
func WithWorkers(n int) Option {
	return func(p *Pool) { p.cfg.Workers = n }
}

// WithQueueSize sets the buffered channel capacity for the job queue. Default: 100.
func WithQueueSize(n int) Option {
	return func(p *Pool) { p.cfg.QueueSize = n }
}

// WithJobTimeout sets the maximum duration of each attempt of a job; with
// retries, every attempt gets its own timeout, and a timed-out attempt is
// retried. Zero means no timeout. Default: 0.
func WithJobTimeout(d time.Duration) Option {
	return func(p *Pool) { p.cfg.JobTimeout = d }
}

// WithMaxRetries sets how many times a failed job is retried. Default: 0 (no retries).
// Errors wrapped with [Permanent] and panics are never retried.
func WithMaxRetries(n int) Option {
	return func(p *Pool) { p.cfg.MaxRetries = n }
}

// WithRetryDelay sets the base delay between retries. Default: 1s.
func WithRetryDelay(d time.Duration) Option {
	return func(p *Pool) { p.cfg.RetryDelay = d }
}

// WithRetryBackoff enables exponential backoff on retries. Default: true.
func WithRetryBackoff(enabled bool) Option {
	return func(p *Pool) { p.cfg.RetryBackoff = enabled }
}

// WithMaxRetryDelay caps the delay between retries, with or without
// exponential backoff. Zero means no cap. Default: 0.
func WithMaxRetryDelay(d time.Duration) Option {
	return func(p *Pool) { p.cfg.MaxRetryDelay = d }
}

// WithRetryJitter randomizes each retry delay by subtracting up to fraction
// of it (0 to 1), so retries of many failing jobs do not happen in lockstep.
// Default: 0 (no jitter).
func WithRetryJitter(fraction float64) Option {
	return func(p *Pool) { p.cfg.RetryJitter = fraction }
}

// WithPanicHandler sets a handler called when a job panics.
// The handler receives the job and the recovered value. The job's error is a
// [*PanicError], which also carries the stack trace.
func WithPanicHandler(fn func(job Job, recovered any)) Option {
	return func(p *Pool) { p.cfg.PanicHandler = fn }
}

// WithLogger sets the structured logger for the pool.
func WithLogger(l Logger) Option {
	return func(p *Pool) { p.logger = l }
}

// WithHooks sets lifecycle hooks for the pool.
func WithHooks(h Hooks) Option {
	return func(p *Pool) { p.hooks = h }
}

// WithMiddleware appends middleware to the processing chain.
// Middleware is applied in the order given (first added = outermost wrapper).
func WithMiddleware(mw ...Middleware) Option {
	return func(p *Pool) { p.middleware = append(p.middleware, mw...) }
}

// Logger is a structured logging interface compatible with [log/slog].
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Hooks contains lifecycle callbacks. All fields are optional. A panic in a
// hook is recovered (and logged if a [Logger] is set) so it cannot crash the
// worker.
type Hooks struct {
	OnJobStart    func(job Job)
	OnJobComplete func(job Job, duration time.Duration)
	OnJobFailed   func(job Job, err error, duration time.Duration)
	OnWorkerStart func(workerID int)
	OnWorkerStop  func(workerID int)
}

// Middleware wraps job processing. Use for cross-cutting concerns like
// logging, tracing, or metrics collection.
//
//	func timingMiddleware(next func(ctx context.Context) error) func(ctx context.Context) error {
//	    return func(ctx context.Context) error {
//	        start := time.Now()
//	        err := next(ctx)
//	        fmt.Println("took", time.Since(start))
//	        return err
//	    }
//	}
type Middleware func(next func(ctx context.Context) error) func(ctx context.Context) error
