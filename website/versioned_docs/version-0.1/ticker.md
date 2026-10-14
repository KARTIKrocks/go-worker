---
id: ticker
title: Ticker
description: Submit a job to a go-worker pool at a fixed interval with Ticker — a lightweight alternative to the scheduler for a single recurring job.
---

# Ticker

`Ticker` submits one job to the pool every `interval`. It is a lighter
alternative to the [scheduler](./scheduler.md) when all you need is "run this
every N seconds".

```go
ticker := worker.NewTickerFunc(pool, 30*time.Second, func(ctx context.Context) error {
    return sendHeartbeat(ctx)
})
ticker.Start()
defer ticker.Stop()
```

| Constructor | First submit |
| --- | --- |
| `NewTicker(pool, interval, job)` | after one `interval` |
| `NewTickerFunc(pool, interval, fn)` | after one `interval` |
| `NewTickerImmediate(pool, interval, job)` | immediately on `Start`, then every `interval` |

The constructors panic if `interval` is not positive. `Start` is idempotent.

## Behaviour

**Fire and forget.** The ticker submits and moves on; it does not wait for the
job. If a run takes longer than the interval, runs overlap — up to the pool's
worker count. Use the scheduler with `OverlapSkip` if they must not.

**Waits for queue space.** If the pool's queue is full, the tick waits for room
before submitting. Like `time.Ticker`, it drops ticks it misses while
waiting, so a backlog of runs does not build up.

**Stopping.** `Stop` stops the ticker and waits for its goroutine to exit. It
never blocks behind a full or paused pool. Runs already submitted keep going:
stopping the ticker does not cancel them.

## Ticker or scheduler?

| Need | Use |
| --- | --- |
| one job on a fixed interval | `Ticker` |
| several named tasks | [Scheduler](./scheduler.md) |
| cron expressions or wall-clock times | Scheduler |
| no overlapping runs | Scheduler with `OverlapSkip` |
| pause, trigger, or inspect at runtime | Scheduler |
