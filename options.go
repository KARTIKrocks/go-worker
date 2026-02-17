package worker

import (
	"context"
	"fmt"
	"time"
)

// Config holds worker pool configuration. Use [Option] functions to set values.
type Config struct {
	Workers       int
	QueueSize     int
	JobTimeout    time.Duration
	MaxRetries    int
	RetryDelay    time.Duration
	MaxRetryDelay time.Duration // 0 means no cap
	RetryBackoff  bool
	PanicHandler  func(job Job, recovered any)
}

func (c *Config) validate() error {
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
	return nil
}

// Option configures a [Pool]. Pass to [NewPool].
type Option func(p *Pool, c *Config)

// WithWorkers sets the number of worker goroutines. Default: 4.
func WithWorkers(n int) Option {
	return func(_ *Pool, c *Config) { c.Workers = n }
}

// WithQueueSize sets the buffered channel capacity for the job queue. Default: 100.
func WithQueueSize(n int) Option {
	return func(_ *Pool, c *Config) { c.QueueSize = n }
}

// WithJobTimeout sets the maximum duration for a single job execution.
// Zero means no timeout. Default: 30s.
func WithJobTimeout(d time.Duration) Option {
	return func(_ *Pool, c *Config) { c.JobTimeout = d }
}

// WithMaxRetries sets how many times a failed job is retried. Default: 0 (no retries).
func WithMaxRetries(n int) Option {
	return func(_ *Pool, c *Config) { c.MaxRetries = n }
}

// WithRetryDelay sets the base delay between retries. Default: 1s.
func WithRetryDelay(d time.Duration) Option {
	return func(_ *Pool, c *Config) { c.RetryDelay = d }
}

// WithRetryBackoff enables exponential backoff on retries. Default: true.
func WithRetryBackoff(enabled bool) Option {
	return func(_ *Pool, c *Config) { c.RetryBackoff = enabled }
}

// WithMaxRetryDelay caps the maximum delay between retries when using
// exponential backoff. Zero means no cap. Default: 0.
func WithMaxRetryDelay(d time.Duration) Option {
	return func(_ *Pool, c *Config) { c.MaxRetryDelay = d }
}

// WithPanicHandler sets a handler called when a job panics.
// The handler receives the job and the recovered value.
func WithPanicHandler(fn func(job Job, recovered any)) Option {
	return func(_ *Pool, c *Config) { c.PanicHandler = fn }
}

// WithLogger sets the structured logger for the pool.
func WithLogger(l Logger) Option {
	return func(p *Pool, _ *Config) { p.logger = l }
}

// WithHooks sets lifecycle hooks for the pool.
func WithHooks(h Hooks) Option {
	return func(p *Pool, _ *Config) { p.hooks = h }
}

// WithMiddleware appends middleware to the processing chain.
// Middleware is applied in the order given (first added = outermost wrapper).
func WithMiddleware(mw ...Middleware) Option {
	return func(p *Pool, _ *Config) { p.middleware = append(p.middleware, mw...) }
}

// Logger is a structured logging interface compatible with [log/slog].
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

// Hooks contains lifecycle callbacks. All fields are optional.
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
