---
id: throttler
title: Throttler
description: Limit a job to at most one run per interval with go-worker's Throttler — leading-edge Submit, trailing-edge SubmitTrailing, Force, and Reset.
---

# Throttler

A throttler runs a job at most once per `interval`. Where a
[debouncer](./debouncer.md) waits for a burst to end, a throttler runs during
the burst — just not more often than the interval allows.

Typical uses: refreshing a dashboard at most once a second while updates
stream in, sending progress notifications, polling an expensive status
endpoint.

```go
throttler := worker.NewThrottler(pool, time.Second)

for event := range events {
    throttler.Submit(worker.JobFunc(func(ctx context.Context) error {
        return publishProgress(ctx, event)
    }))
}
```

## Leading and trailing

**`Submit(job) bool`** is leading-edge: if at least `interval` has passed since
the last run, it submits the job, starts a new window, and returns `true`.
Otherwise it drops the job and returns `false`.

Leading-edge alone has a weakness: the last event of a burst is usually
dropped, so the final state is never published. **`SubmitTrailing(job)`**
fixes that: within a window, it remembers the job and runs it when the window
ends. Repeated calls within the window replace the pending job, so only the
most recent runs.

```go
for event := range events {
    throttler.SubmitTrailing(worker.JobFunc(func(ctx context.Context) error {
        return publishProgress(ctx, event) // the latest event is always published
    }))
}
```

If the window is already open, `SubmitTrailing` submits immediately.

## Methods

| Method | Effect |
| --- | --- |
| `Submit(job) bool` | run now if the window is open; otherwise drop |
| `SubmitTrailing(job)` | run now if the window is open; otherwise run at the end of the window |
| `Force(job) error` | run now regardless, start a new window, drop any pending trailing job |
| `Reset()` | open the window and drop any pending trailing job |

## Things to know

- **`Submit` returns `false` for two reasons:** the window is closed, or the
  pool rejected the job. In the second case the window was still used.
- **Submits wait for queue space.** The throttler submits with `SubmitJob`, so
  a full queue blocks the caller (or, for trailing jobs, the timer goroutine).
- **Trailing-job submit errors are dropped.** `Force` is the only method that
  returns the submit error.
