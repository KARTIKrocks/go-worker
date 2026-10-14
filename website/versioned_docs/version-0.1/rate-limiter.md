---
id: rate-limiter
title: Rate Limiter
description: Cap how many jobs per interval are submitted to a go-worker pool with RateLimiter — waiting Submit, non-blocking TrySubmit, and Stop.
---

# Rate Limiter

Worker count limits how many jobs run **at once**. `RateLimiter` limits how
many are **submitted per unit of time** — useful when the downstream system has
a rate limit of its own, such as an API that allows 10 calls per second.

Tokens are spent at submission, not when a worker starts the job. If jobs
back up in the queue, they can reach the downstream system in bursts above
the rate. Keep the queue short, or enforce the limit inside the job when the
downstream limit is strict.

```go
rl := worker.NewRateLimiter(pool, 10, time.Second) // 10 jobs per second
defer rl.Stop()

for _, msg := range messages {
    err := rl.Submit(ctx, worker.JobFunc(func(ctx context.Context) error {
        return smsAPI.Send(ctx, msg)
    }))
    if err != nil {
        return err
    }
}
```

`NewRateLimiter(pool, n, interval)` panics if `n < 1` or `interval <= 0`.

## How tokens work

The limiter holds up to `n` tokens and starts full. Every submit spends one.
Every `interval`, the bucket is topped back up to `n`.

Because it refills in steps rather than continuously, up to `n` jobs can start
back to back at the start of each interval. If the downstream limit is strict
about spacing, use a smaller `n` over a shorter interval: `1` per `100ms`
rather than `10` per second.

## Submitting

| Method | Without a token | Returns |
| --- | --- | --- |
| `Submit(ctx, job)` | waits for one, or for `ctx` to end | `nil`, `ctx.Err()`, a pool error, or `ErrLimiterStopped` |
| `TrySubmit(job)` | returns `ErrRateLimited` | `nil`, `ErrRateLimited`, a pool error, or `ErrLimiterStopped` |

`Submit` submits with [`SubmitContext`](./pool.md#bounded-waiting), so the job
runs with a context derived from `ctx`. If `ctx` is cancelled after the job is
queued, the job is cancelled too.

If the pool rejects a job — `ErrPoolFull` from `TrySubmit`, `ErrPoolClosed`
from either — the token is returned to the bucket.

## Stopping

`Stop` stops the refill goroutine. Afterwards, both submit methods return
`ErrLimiterStopped`, including a `Submit` that was waiting for a token. Always
stop a limiter you no longer need, or its goroutine runs for the life of the
process.

## Rate limiting HTTP traffic?

`RateLimiter` limits jobs going **into a pool**. To limit requests coming into
an HTTP server, per client, see
[go-ratelimit](https://kartikrocks.github.io/go-ratelimit/).
