---
id: lifecycle
title: Lifecycle
description: Pausing and resuming a go-worker pool, graceful shutdown with Close, and forced shutdown with CloseWithTimeout.
---

# Lifecycle

## Pause and resume

```go
pool.Pause()  // workers finish their current job, then stop picking up new ones
pool.Resume() // workers carry on
```

While paused, submits are still accepted until the queue fills, so producers
keep running and the backlog waits for `Resume`. Use it to stop consuming while
a dependency is down — a database failover, a rate-limited upstream — without
losing queued work.

`IsPaused()` reports the state. Pausing a closing pool does nothing, so a
pause cannot stall shutdown.

## Graceful shutdown

`Close` stops accepting jobs and waits for **every queued and running job** to
finish:

```go
pool.Close()
```

In order, it:

1. marks the pool closed, so new submits return `ErrPoolClosed`,
2. releases any submit that was blocked waiting for queue space, with
   `ErrPoolClosed`,
3. resumes the pool if it was paused, so the queue can drain,
4. waits for the workers to work through the queue and exit.

`Close` is idempotent and always returns `nil`. Call it with `defer` right
after creating the pool.

## Bounded shutdown

A full queue of slow jobs can make `Close` take a long time — longer than your
orchestrator's termination grace period. `CloseWithTimeout` drains for up to
the timeout, then forces the issue:

```go
// Kubernetes sends SIGTERM and kills the pod 30s later.
pool.CloseWithTimeout(25 * time.Second)
```

When the timeout expires:

- **running jobs** have their context cancelled,
- **queued jobs** that never started are dropped,
- anyone waiting on a dropped job — `SubmitWait`, a future, a group — receives
  `ErrPoolClosed`.

`CloseWithTimeout` then waits for the running jobs to return. A job that
ignores its context will hold up shutdown regardless of the timeout, which is
one more reason to pass the context to every blocking call.

A timeout of `0` is the same as `Close`.

## A complete shutdown

```go
func main() {
    pool, err := worker.NewPool(worker.WithWorkers(8))
    if err != nil {
        log.Fatal(err)
    }

    sched := worker.NewScheduler(pool)
    sched.EveryFunc("cleanup", 5*time.Minute, cleanup)
    sched.Start()

    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()
    <-ctx.Done()

    sched.Stop()                         // stop producing new work first
    pool.CloseWithTimeout(20 * time.Second) // then drain what is left
}
```

Stop the producers — schedulers, tickers, HTTP servers — before closing the
pool. Otherwise they keep submitting into a closing pool and log
`ErrPoolClosed` errors during every deploy.
