// Package worker provides a high-performance, zero-dependency worker pool
// for concurrent job processing in Go.
//
// # Features
//
//   - Configurable worker count and bounded job queue
//   - Graceful shutdown with optional timeout
//   - Automatic retries with exponential backoff
//   - Panic recovery (workers survive panics)
//   - Middleware chain for cross-cutting concerns
//   - Lifecycle hooks for observability
//   - Atomic metrics (submitted, completed, failed, panicked, active, latency)
//   - Pause/Resume for flow control
//   - ErrorGroup and Group for fan-out/fan-in patterns
//   - Generic Batch processor for bulk operations
//   - Generic Future for typed async results
//   - Scheduler with cron, interval, daily, and one-shot schedules
//   - Ticker, Debouncer, Throttler, and RateLimiter helpers
//   - Zero external dependencies
//
// # Quick Start
//
//	pool, err := worker.NewPool(
//	    worker.WithWorkers(8),
//	    worker.WithQueueSize(500),
//	    worker.WithMaxRetries(3),
//	)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer pool.Close()
//
//	// Fire and forget
//	pool.Submit(func(ctx context.Context) error {
//	    return doWork(ctx)
//	})
//
//	// Wait for result
//	err = pool.SubmitWait(ctx, func(ctx context.Context) error {
//	    return doWork(ctx)
//	})
//
//	// Typed async result
//	future := worker.SubmitTyped(pool, func(ctx context.Context) (int, error) {
//	    return compute(ctx)
//	})
//	val, err := future.Await(ctx)
package worker
