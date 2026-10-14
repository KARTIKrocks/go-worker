<!-- The centred logo block opens the file, so there is no h1 on line 1. -->
<!-- markdownlint-disable-next-line MD041 -->
<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="website/static/img/logo-dark.svg">
    <img src="website/static/img/logo.svg" alt="go-worker" width="104" height="104">
  </picture>
</p>

<h1 align="center">go-worker</h1>

<p align="center">
  A high-performance, zero-dependency worker pool for Go with retries, panic
  recovery, graceful shutdown, typed futures, error groups, batching, and a
  cron scheduler.
</p>

<p align="center">
  <a href="https://pkg.go.dev/github.com/KARTIKrocks/go-worker"><img src="https://pkg.go.dev/badge/github.com/KARTIKrocks/go-worker.svg" alt="Go Reference"></a>
  <a href="https://github.com/KARTIKrocks/go-worker/releases"><img src="https://img.shields.io/github/v/tag/KARTIKrocks/go-worker?filter=v*&amp;label=release" alt="Latest release"></a>
  <a href="go.mod"><img src="https://img.shields.io/github/go-mod/go-version/KARTIKrocks/go-worker" alt="Go version"></a>
  <a href="https://github.com/KARTIKrocks/go-worker/actions/workflows/ci.yml"><img src="https://github.com/KARTIKrocks/go-worker/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://codecov.io/gh/KARTIKrocks/go-worker"><img src="https://codecov.io/gh/KARTIKrocks/go-worker/branch/main/graph/badge.svg" alt="Coverage"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/KARTIKrocks/go-worker" alt="License: MIT"></a>
</p>

<p align="center">
  <b><a href="https://kartikrocks.github.io/go-worker/">Documentation</a></b> ·
  <b><a href="https://pkg.go.dev/github.com/KARTIKrocks/go-worker">API Reference</a></b> ·
  <b><a href="CHANGELOG.md">Changelog</a></b>
</p>

## Why go-worker?

A `go` statement and a `sync.WaitGroup` get you concurrency. Running background
work safely in production — bounded, retried, cancellable, observable, and shut
down cleanly — is everything past that:

| Capability | go-worker | Goroutines + WaitGroup |
| - | - | - |
| Bounded concurrency and backpressure | ✓ | You build it |
| Retries with backoff, jitter, and permanent errors | ✓ | You build it |
| Panic recovery with stack traces | ✓ | You build it |
| Per-attempt timeouts and cancellation | ✓ | You build it |
| Graceful drain and forced shutdown | ✓ | You build it |
| Cron, interval, and one-shot scheduling | ✓ | You build it |
| Debounce, throttle, and rate limiting | ✓ | You build it |
| Metrics and lifecycle hooks | ✓ | You build it |

All of it in the standard library only — nothing to audit, nothing to update.

## Performance

```text
Submit            0.9 µs/op    5 allocs   (sustained, full queue)
SubmitWait        1.5 µs/op    8 allocs   (submit, run, and hand back the result)
Throughput       ~1.1M jobs/s             (no-op jobs, 8 workers)
```

An 8-worker pool with 12 goroutines submitting no-op jobs — so these are the
pool's own overhead, not your jobs'. Reproduce with
`go test -bench=. -benchmem ./...`; full numbers in [Benchmarks](#benchmarks)
below.

## Features

- **Worker Pool**: A fixed set of goroutines and a bounded queue, with blocking, non-blocking, and context-aware submits
- **Retries**: Exponential backoff, a delay cap, jitter, per-attempt timeouts, and `Permanent` errors that skip retrying
- **Panic Recovery**: A panicking job fails with a `*PanicError` carrying its stack trace; the worker keeps running
- **Graceful Shutdown**: `Close` drains the queue; `CloseWithTimeout` force-cancels after a deadline
- **Pause/Resume**: Stop picking up new work without shutting down
- **Middleware**: Wrap every attempt for logging, tracing, or metrics
- **Lifecycle Hooks**: Callbacks for job start/complete/fail and worker start/stop
- **Typed Futures**: Generic async results with `SubmitTyped` and `Future[T]`
- **ErrorGroup & Group**: Fan-out/fan-in with cancel-on-first-error or full error collection
- **Batch**: Generic chunked processing for bulk operations
- **Scheduler**: Cron expressions, intervals, daily times, and one-shot tasks with overlap policies and jitter
- **Ticker / Debouncer / Throttler / RateLimiter**: Common flow-control patterns, built on the pool
- **Metrics**: Atomic counters, success rate, and throughput
- **Zero Dependencies**: Standard library only

## Installation

```bash
go get github.com/KARTIKrocks/go-worker
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/KARTIKrocks/go-worker"
)

func main() {
    pool, err := worker.NewPool(
        worker.WithWorkers(8),
        worker.WithQueueSize(500),
        worker.WithMaxRetries(3),
        worker.WithJobTimeout(time.Minute), // per attempt
    )
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close() // drains queued and running jobs

    // Fire and forget
    if err := pool.Submit(func(ctx context.Context) error {
        fmt.Println("hello from a worker")
        return nil
    }); err != nil {
        log.Printf("submit: %v", err) // ErrPoolClosed
    }

    // Wait for the result, after retries
    if err := pool.SubmitWait(context.Background(), sendEmail); err != nil {
        log.Printf("email failed: %v", err)
    }

    // Typed async result
    future := worker.SubmitTyped(pool, countUsers)
    n, err := future.Await(context.Background())
    if err != nil {
        log.Printf("count failed: %v", err)
    }
    fmt.Println("users:", n)
}
```

Recurring work:

```go
sched := worker.NewScheduler(pool)
sched.EveryFunc("cleanup", 5*time.Minute, cleanup)
if err := sched.CronFunc("report", "0 9 * * MON-FRI", sendReport); err != nil {
    log.Fatal(err) // invalid cron expression
}
sched.Start()
defer sched.Stop()
```

> **Note:** go-worker is pre-1.0, and `v0.1.0` changed several defaults — most
> notably, `JobTimeout` now defaults to no timeout and applies to each attempt.
> See the [changelog](CHANGELOG.md) when upgrading.

## Documentation

Full guides live at **[kartikrocks.github.io/go-worker](https://kartikrocks.github.io/go-worker/)**:

| Guide | Covers |
| --- | --- |
| [Getting Started](https://kartikrocks.github.io/go-worker/docs/getting-started) | Create a pool and submit work |
| [Worker Pool](https://kartikrocks.github.io/go-worker/docs/pool) | Every way to submit, and what each waits for |
| [Configuration](https://kartikrocks.github.io/go-worker/docs/configuration) | Every option and its default |
| [Retries and Panics](https://kartikrocks.github.io/go-worker/docs/retries) | What is retried, backoff, jitter, `PanicError` |
| [Lifecycle](https://kartikrocks.github.io/go-worker/docs/lifecycle) | Pause, resume, graceful and forced shutdown |
| [Middleware](https://kartikrocks.github.io/go-worker/docs/middleware) | Wrapping every attempt |
| [Hooks](https://kartikrocks.github.io/go-worker/docs/hooks) | Job and worker lifecycle callbacks |
| [Future](https://kartikrocks.github.io/go-worker/docs/future) | Typed async results |
| [Groups](https://kartikrocks.github.io/go-worker/docs/groups) | `Group` and `ErrorGroup` fan-out |
| [Batch](https://kartikrocks.github.io/go-worker/docs/batch) | Chunked bulk processing |
| [Scheduler](https://kartikrocks.github.io/go-worker/docs/scheduler) | Tasks, overlap policies, runtime control |
| [Schedules and Cron](https://kartikrocks.github.io/go-worker/docs/schedules) | Cron syntax, time zones, custom schedules |
| [Ticker](https://kartikrocks.github.io/go-worker/docs/ticker), [Debouncer](https://kartikrocks.github.io/go-worker/docs/debouncer), [Throttler](https://kartikrocks.github.io/go-worker/docs/throttler), [Rate Limiter](https://kartikrocks.github.io/go-worker/docs/rate-limiter) | Flow-control helpers |
| [Metrics](https://kartikrocks.github.io/go-worker/docs/metrics) | `Snapshot`, derived rates, Prometheus export |
| [Errors](https://kartikrocks.github.io/go-worker/docs/errors) | Sentinel errors and matching |

Exact type signatures are generated from source on
[pkg.go.dev](https://pkg.go.dev/github.com/KARTIKrocks/go-worker).

Runnable programs are in [`examples/`](examples/) — basic, retries, middleware,
future, errorgroup, batch, scheduler, rate-limiter, and graceful-shutdown.

## Benchmarks

Measured on an Intel i5-11400H @ 2.70GHz (12 cores), Go 1.27, Linux; median of
three runs. Each benchmark uses an 8-worker pool with 12 goroutines submitting
no-op jobs in parallel.

```bash
go test -bench=. -benchmem ./...
```

| Benchmark | ns/op | B/op | Allocs |
| --- | --- | --- | --- |
| `Submit` | 909 | 248 | 5 |
| `SubmitWait` | 1,473 | 392 | 8 |
| `SubmitWait` with one middleware | 1,466 | 440 | 11 |
| `TrySubmit` | 60 | 7 | 0 |

`Submit` runs with the queue full, so its figure is the pool's steady-state
throughput. `TrySubmit` mostly measures the rejection path: with the queue full,
most calls return `ErrPoolFull` immediately.

## Thread Safety

Every `Pool`, `Scheduler`, `Future`, group, and flow-control helper method is
safe for concurrent use. `Batch` is the exception: do not call `Add` while
`Process` runs.

## Security

Every push and pull request to `main` is scanned by
[CodeQL](https://github.com/KARTIKrocks/go-worker/actions/workflows/codeql.yml),
and `govulncheck` gates every merge on advisories reachable from this code's
call graph. go-worker has no third-party dependencies, so that scan covers the
standard library it uses; Dependabot tracks the GitHub Actions and the docs
site.

Please report vulnerabilities privately through
[GitHub Security Advisories](https://github.com/KARTIKrocks/go-worker/security/advisories/new),
not public issues.

## License

[MIT](LICENSE)

## Contributing

Contributions welcome — open an
[issue](https://github.com/KARTIKrocks/go-worker/issues) to discuss a change, or
send a pull request. Run `make ci` before opening one.
