---
id: hooks
title: Hooks
description: Lifecycle callbacks for go-worker jobs and workers — OnJobStart, OnJobComplete, OnJobFailed, OnWorkerStart, OnWorkerStop — and when each fires.
---

# Hooks

Hooks are callbacks for job and worker lifecycle events. Set any subset of
them with `WithHooks`:

```go
pool, _ := worker.NewPool(worker.WithHooks(worker.Hooks{
    OnJobStart: func(job worker.Job) {
        inFlight.Inc()
    },
    OnJobComplete: func(job worker.Job, d time.Duration) {
        inFlight.Dec()
        jobDuration.Observe(d.Seconds())
    },
    OnJobFailed: func(job worker.Job, err error, d time.Duration) {
        inFlight.Dec()
        jobFailures.Inc()
        slog.Error("job failed", "err", err, "took", d)
    },
    OnWorkerStart: func(id int) { slog.Debug("worker started", "id", id) },
    OnWorkerStop:  func(id int) { slog.Debug("worker stopped", "id", id) },
}))
```

## When hooks fire

| Hook | Fires | Receives |
| --- | --- | --- |
| `OnJobStart` | once per job, when a worker picks it up | the job |
| `OnJobComplete` | once per job, when it finally succeeds | the job, total duration |
| `OnJobFailed` | once per job, when it finally fails | the job, final error, total duration |
| `OnWorkerStart` | when a worker goroutine starts | worker ID, `0` to `workers − 1` |
| `OnWorkerStop` | when a worker goroutine exits | worker ID |

Job hooks fire **once per job**, not per attempt. The duration covers every
attempt and the delays between them. Exactly one of `OnJobComplete` and
`OnJobFailed` fires for every job that started. For per-attempt behaviour, use
[middleware](./middleware.md).

A job dropped by `CloseWithTimeout` before it started never fires any hook.

## Identifying jobs

Hooks receive the `Job`. For a job submitted as a function, that is a
`JobFunc`, which carries no identity. To tell jobs apart in hooks, submit a
type that implements `Job` and switch on it:

```go
OnJobFailed: func(job worker.Job, err error, d time.Duration) {
    if j, ok := job.(*ResizeImage); ok {
        slog.Error("resize failed", "image", j.ID, "err", err)
    }
},
```

## Rules for hooks

- **Keep them fast.** Hooks run on the worker goroutine, so a slow hook holds
  up the next job.
- **They may run concurrently.** Several workers call the same hook at once;
  shared state needs synchronizing.
- **Panics are recovered.** A panicking hook is logged (if a logger is set)
  and otherwise ignored. It cannot crash the worker.
