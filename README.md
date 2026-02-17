# go-worker

[![Go Reference](https://pkg.go.dev/badge/github.com/KARTIKrocks/go-worker.svg)](https://pkg.go.dev/github.com/KARTIKrocks/go-worker)
[![Go Report Card](https://goreportcard.com/badge/github.com/KARTIKrocks/go-worker)](https://goreportcard.com/report/github.com/KARTIKrocks/go-worker)

A high-performance, zero-dependency worker pool for Go with retries, scheduling, and observability.

## Features

- **Worker Pool** — configurable goroutine pool with bounded queue
- **Retries** — automatic retries with exponential backoff
- **Panic Recovery** — workers survive panics and keep processing
- **Middleware** — composable chain for logging, tracing, metrics
- **Lifecycle Hooks** — callbacks for job start/complete/fail and worker start/stop
- **Metrics** — atomic counters for submitted/completed/failed/panicked/active/latency
- **Pause/Resume** — flow control without shutdown
- **ErrorGroup & Group** — fan-out/fan-in with error collection or cancel-on-first-error
- **Batch** — generic batch processor for bulk operations
- **Future** — generic typed async results
- **Scheduler** — cron expressions, fixed intervals, daily times, one-shot
- **Ticker / Debouncer / Throttler / RateLimiter** — common patterns built-in
- **Zero Dependencies** — stdlib only

## Install

```bash
go get github.com/KARTIKrocks/go-worker
```

Requires Go 1.23+.

## Quick Start

```go
pool, err := worker.NewPool(
    worker.WithWorkers(8),
    worker.WithQueueSize(500),
)
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

// Fire and forget
pool.Submit(func(ctx context.Context) error {
    fmt.Println("Hello from worker!")
    return nil
})

// Wait for completion
err = pool.SubmitWait(ctx, func(ctx context.Context) error {
    return doExpensiveWork(ctx)
})
```

## Configuration

All configuration uses functional options:

```go
pool, err := worker.NewPool(
    worker.WithWorkers(8),           // goroutine count (default: 4)
    worker.WithQueueSize(500),       // buffered channel size (default: 100)
    worker.WithJobTimeout(time.Minute), // per-job timeout (default: 30s, 0=none)
    worker.WithMaxRetries(3),        // retry failed jobs (default: 0)
    worker.WithRetryDelay(time.Second), // base retry delay (default: 1s)
    worker.WithRetryBackoff(true),      // exponential backoff (default: true)
    worker.WithMaxRetryDelay(time.Minute), // cap retry delay (default: 0=no cap)
    worker.WithPanicHandler(func(job worker.Job, r any) {
        log.Printf("panic: %v", r)
    }),
    worker.WithLogger(slogLogger),   // any Logger interface
    worker.WithMiddleware(mw1, mw2), // middleware chain
    worker.WithHooks(worker.Hooks{
        OnJobStart:    func(j worker.Job) { /* ... */ },
        OnJobComplete: func(j worker.Job, d time.Duration) { /* ... */ },
        OnJobFailed:   func(j worker.Job, err error, d time.Duration) { /* ... */ },
        OnWorkerStart: func(id int) { /* ... */ },
        OnWorkerStop:  func(id int) { /* ... */ },
    }),
)
```

## Submitting Jobs

```go
// Function (fire and forget)
pool.Submit(func(ctx context.Context) error {
    return doWork(ctx)
})

// Function (wait for result)
err := pool.SubmitWait(ctx, func(ctx context.Context) error {
    return doWork(ctx)
})

// Non-blocking (returns ErrPoolFull if queue is full)
err := pool.TrySubmit(func(ctx context.Context) error {
    return doWork(ctx)
})

// With context (blocks until enqueued or ctx expires)
err := pool.SubmitContext(ctx, myJob)

// Job interface
type MyJob struct{ ID string }
func (j *MyJob) Process(ctx context.Context) error { return nil }
pool.SubmitJob(&MyJob{ID: "1"})
```

## Typed Results (Future)

```go
future := worker.SubmitTyped(pool, func(ctx context.Context) (int, error) {
    return computeSomething(ctx)
})

val, err := future.Await(ctx)

if future.Done() {
    // result is ready
}
```

## Error Group

Cancel all jobs on first error (like `errgroup.Group`):

```go
g := worker.NewErrorGroup(pool)

g.Go(func(ctx context.Context) error { return fetchA(ctx) })
g.Go(func(ctx context.Context) error { return fetchB(ctx) })
g.Go(func(ctx context.Context) error { return fetchC(ctx) })

err := g.Wait() // first error, or nil
```

## Group (collect all errors)

```go
g := worker.NewGroup(pool)

g.Go(func(ctx context.Context) error { return task1(ctx) })
g.Go(func(ctx context.Context) error { return task2(ctx) })

errs := g.WaitAll() // []error
```

## Batch Processing

```go
batch := worker.NewBatch(pool, 100, func(ctx context.Context, users []User) error {
    return db.BulkInsert(ctx, users)
})

batch.Add(users...)
err := batch.Process(ctx) // cancels remaining on first error

// Or collect all errors:
errs := batch.ProcessAll(ctx)
```

## Scheduler

```go
sched := worker.NewScheduler(pool)

// Fixed interval
sched.EveryFunc("cleanup", 5*time.Minute, cleanup)

// Cron expression
sched.CronFunc("report", "0 9 * * 1-5", generateReport)

// Daily at specific times
sched.DailyFunc("backup", []string{"02:00", "14:00"}, backup)

// One-shot
sched.OnceFunc("migrate", targetTime, migrate)

// With options
sched.EveryFunc("sync", time.Hour, syncData,
    worker.WithRunImmediate(),
    worker.WithJitter(30*time.Second),
    worker.WithOverlapPolicy(worker.OverlapSkip),
    worker.WithMaxRuns(10),
)

sched.Start()
defer sched.Stop()

// Runtime control
sched.Pause("sync")
sched.Resume("sync")
sched.Trigger("sync")  // manual fire
sched.Remove("sync")
info := sched.TaskInfo("cleanup")
```

## Middleware

```go
func timingMiddleware(next func(ctx context.Context) error) func(ctx context.Context) error {
    return func(ctx context.Context) error {
        start := time.Now()
        err := next(ctx)
        log.Printf("job took %v", time.Since(start))
        return err
    }
}

pool, _ := worker.NewPool(worker.WithMiddleware(timingMiddleware))
```

## Metrics

```go
snap := pool.Snapshot()

fmt.Printf("Submitted:  %d\n", snap.JobsSubmitted)
fmt.Printf("Completed:  %d\n", snap.JobsCompleted)
fmt.Printf("Failed:     %d\n", snap.JobsFailed)
fmt.Printf("Panicked:   %d\n", snap.JobsPanicked)
fmt.Printf("Retried:    %d\n", snap.JobsRetried)
fmt.Printf("Active:     %d\n", snap.ActiveWorkers)
fmt.Printf("Queued:     %d\n", snap.QueueLength)
fmt.Printf("Avg Dur:    %v\n", snap.AverageJobDuration())
fmt.Printf("Success:    %.1f%%\n", snap.SuccessRate()*100)
fmt.Printf("Throughput: %.1f/s\n", snap.Throughput())
```

## Pool Control

```go
pool.Pause()   // workers finish current job, then wait
pool.Resume()  // workers continue

pool.Close()   // graceful shutdown (waits for all jobs)
pool.CloseWithTimeout(30 * time.Second) // force after timeout
```

## Helpers

```go
// Ticker — submit a job at fixed intervals
ticker := worker.NewTickerFunc(pool, time.Minute, heartbeat)
ticker.Start()
defer ticker.Stop()

// Debouncer — only execute the last call after a quiet period
debouncer := worker.NewDebouncer(pool, 500*time.Millisecond)
debouncer.Submit(job) // only last one within 500ms runs

// Throttler — at most one execution per interval
throttler := worker.NewThrottler(pool, time.Second)
throttler.Submit(job) // runs at most 1/sec

// RateLimiter — token bucket
rl := worker.NewRateLimiter(pool, 10, time.Second) // 10 jobs/sec
rl.Submit(ctx, job)
defer rl.Stop()
```

## Errors

```go
errors.Is(err, worker.ErrPoolClosed)    // pool was closed
errors.Is(err, worker.ErrPoolFull)      // queue is full (TrySubmit)
errors.Is(err, worker.ErrJobPanic)      // job panicked
errors.Is(err, worker.ErrInvalidConfig) // bad configuration
```

## License

MIT
