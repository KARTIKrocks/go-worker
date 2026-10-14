---
id: configuration
title: Configuration
description: Every go-worker pool option, its default, and the validation NewPool applies.
---

# Configuration

Pools are configured with functional options passed to `NewPool`:

```go
pool, err := worker.NewPool(
    worker.WithWorkers(8),
    worker.WithQueueSize(500),
    worker.WithJobTimeout(30*time.Second),
    worker.WithMaxRetries(3),
    worker.WithRetryDelay(500*time.Millisecond),
    worker.WithMaxRetryDelay(10*time.Second),
    worker.WithRetryJitter(0.2),
    worker.WithLogger(slog.Default()),
)
```

## Options

| Option | Default | Effect |
| --- | --- | --- |
| `WithWorkers(n)` | `4` | number of worker goroutines; must be at least 1 |
| `WithQueueSize(n)` | `100` | capacity of the job queue; `0` means no buffer |
| `WithJobTimeout(d)` | `0` (none) | maximum duration of **each attempt** |
| `WithMaxRetries(n)` | `0` | how many times a failed job is retried |
| `WithRetryDelay(d)` | `1s` | base delay before the first retry |
| `WithRetryBackoff(enabled)` | `true` | double the delay after every attempt |
| `WithMaxRetryDelay(d)` | `0` (no cap) | upper bound on any single retry delay |
| `WithRetryJitter(fraction)` | `0` | randomly shorten each delay by up to this fraction, `0` to `1` |
| `WithPanicHandler(fn)` | none | called with the job and the recovered value when a job panics |
| `WithLogger(l)` | none | structured logger for pool events |
| `WithHooks(h)` | none | lifecycle callbacks — see [Hooks](./hooks.md) |
| `WithMiddleware(mw...)` | none | wrap every attempt — see [Middleware](./middleware.md) |

How the retry options combine is covered in [Retries and Panics](./retries.md).

## Validation

`NewPool` returns an error wrapping `ErrInvalidConfig` when an option is out of
range:

- workers below 1,
- a negative queue size, job timeout, retry count, retry delay, or max retry
  delay,
- a retry jitter outside `[0, 1]` (including `NaN`).

```go
pool, err := worker.NewPool(worker.WithWorkers(cfg.Workers))
if errors.Is(err, worker.ErrInvalidConfig) {
    log.Fatalf("bad worker config: %v", err)
}
```

The error message names the offending option and value. Options are applied
in order, so a later option overrides an earlier one.

## Logger

`WithLogger` accepts anything with `Debug`, `Info`, `Warn`, and `Error` methods
taking `(msg string, args ...any)` — which `*slog.Logger` already satisfies:

```go
worker.WithLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
```

The pool logs startup and shutdown and pause changes at `Info`, each retry at
`Warn`, and recovered panics at `Error` with the stack. Without a logger it
logs nothing.

## Job timeout

`WithJobTimeout` limits **each attempt**, not the whole job. With three retries
and a one-minute timeout, a job can run for up to four minutes plus the retry
delays. An attempt that returns an error after timing out — normally
`context.DeadlineExceeded` — is retried like any other failure.

The timeout works by cancelling the job's context. A job that ignores its
context keeps running after the deadline and still occupies its worker, so
pass the context to every blocking call.
