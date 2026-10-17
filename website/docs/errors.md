---
id: errors
title: Errors
description: Sentinel errors returned by go-worker, PanicError, Permanent errors, and matching them with errors.Is and errors.As.
---

# Errors

## Sentinel errors

| Error | Returned by | Meaning |
| --- | --- | --- |
| `ErrPoolClosed` | every submit, and waiters on dropped jobs | the pool is closed or closing |
| `ErrPoolFull` | `TrySubmit`, `TrySubmitJob`, `RateLimiter.TrySubmit` | the queue is full |
| `ErrInvalidConfig` | `NewPool` | an option is out of range; wrapped with details |
| `ErrJobPanic` | jobs that panicked | matched by every `*PanicError` |
| `ErrRateLimited` | `RateLimiter.TrySubmit` | no token is available |
| `ErrLimiterStopped` | `RateLimiter.Submit`, `RateLimiter.TrySubmit` | the rate limiter was stopped |

Match them with `errors.Is`:

```go
switch err := pool.TrySubmit(job); {
case err == nil:
case errors.Is(err, worker.ErrPoolFull):
    return errBusy
case errors.Is(err, worker.ErrPoolClosed):
    return errShuttingDown
}
```

## Errors from jobs

A job's own error is returned unchanged by `SubmitWait`, futures, and groups —
after all [retries](./retries.md) — so match it as you normally would.
Two wrappers can appear around it:

| Type | Created by | Notes |
| --- | --- | --- |
| `*PanicError` | the pool, when a job panics | matches `ErrJobPanic`; `Value` and `Stack` hold the panic; unwraps to `Value` if it is an error |
| permanent error | `worker.Permanent(err)` | stops retries; transparent to `errors.Is` and `errors.As` |

```go
if perr, ok := errors.AsType[*worker.PanicError](err); ok {
    log.Printf("panic: %v\n%s", perr.Value, perr.Stack)
}
```

## Context errors

Waiting operations return `ctx.Err()` — `context.Canceled` or
`context.DeadlineExceeded` — when their context ends first:

- `SubmitContext`, `SubmitWait`, `SubmitJobWait`, `RateLimiter.Submit`, and
  `Future.Await`, when the caller's context ends,
- a job's result, when its attempt timed out on the last attempt, or its
  context was cancelled by the caller, a group, or a forced close. If the job
  returned its own error, the result matches that error too.

## Panics from constructors

`NewTicker` and `NewRateLimiter` panic on a non-positive interval or count,
since those are programming errors. `NewPool` returns `ErrInvalidConfig`
instead, because pool sizes often come from configuration.
