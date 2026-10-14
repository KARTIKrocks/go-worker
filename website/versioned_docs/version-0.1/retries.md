---
id: retries
title: Retries and Panics
description: How go-worker retries failed jobs — backoff, delay caps, jitter, per-attempt timeouts, permanent errors — and how panics are recovered and reported.
---

# Retries and Panics

## Retrying

With `WithMaxRetries(n)`, a job that returns an error is run again, up to `n`
more times. The job's final error — from its last attempt — is what
`SubmitWait`, futures, groups, and hooks see.

```go
pool, _ := worker.NewPool(
    worker.WithMaxRetries(4),
    worker.WithRetryDelay(200*time.Millisecond),
)
```

### What is not retried

A failed job is retried unless:

| Condition | Why |
| --- | --- |
| it returned an error wrapped with `worker.Permanent` | you said retrying will not help |
| it panicked | a panic is a bug; running it again rarely fixes it |
| its context was cancelled | the caller gave up, or the pool is force-closing |

A **timed-out attempt is retried**: `WithJobTimeout` limits each attempt
separately, so a slow attempt gets another go. Cancellation that comes from
outside the attempt — the `SubmitWait` caller's context ending, a group being
cancelled, `CloseWithTimeout` expiring — stops the retries.

### Permanent errors

Wrap an error with `Permanent` to fail the job immediately:

```go
pool.Submit(func(ctx context.Context) error {
    user, err := db.GetUser(ctx, id)
    if errors.Is(err, sql.ErrNoRows) {
        return worker.Permanent(err) // retrying will not find it
    }
    if err != nil {
        return err // transient — retried
    }
    return notify(ctx, user)
})
```

The wrapper is transparent: `errors.Is` and `errors.As` still see the original
error, and its message is unchanged. `Permanent(nil)` is `nil`.

## Retry delays

The delay before retry *k* (the first retry is *k* = 1) is computed in three
steps:

1. **Backoff.** With `WithRetryBackoff(true)` (the default), the delay is
   `RetryDelay × 2^(k−1)`: 1s, 2s, 4s, 8s… With backoff off, it is always
   `RetryDelay`.
2. **Cap.** If `WithMaxRetryDelay` is set, the delay is limited to it.
3. **Jitter.** With `WithRetryJitter(f)`, a random amount up to `f` of the
   delay is subtracted, so it lands between `(1 − f) × delay` and `delay`.

Jitter only ever shortens a delay, so the cap is never exceeded. Use it when
many jobs can fail at once — a shared dependency going down — so their retries
do not all hit it again in the same instant.

| Retry | Delay (1s, backoff, 5s cap) | With 0.5 jitter |
| --- | --- | --- |
| 1 | 1s | 0.5s – 1s |
| 2 | 2s | 1s – 2s |
| 3 | 4s | 2s – 4s |
| 4 | 5s | 2.5s – 5s |

The wait between attempts is interrupted if the job's context is cancelled.

### What retries cost

A job holds its worker for its whole life, **including the delays between
attempts**. Four retries with long backoff can occupy a worker for a minute
while doing nothing. With long delays and many failures, either raise the
worker count or cap delays with `WithMaxRetryDelay`.

## Panics

A panicking job does not crash the worker or the process. The panic is
recovered, and the job fails with a `*PanicError`:

```go
err := pool.SubmitWait(ctx, func(ctx context.Context) error {
    var m map[string]int
    m["boom"] = 1 // panics
    return nil
})

if perr, ok := errors.AsType[*worker.PanicError](err); ok {
    log.Printf("job panicked: %v\n%s", perr.Value, perr.Stack)
}
```

| Field | Contents |
| --- | --- |
| `Value` | the value passed to `panic` |
| `Stack` | the stack trace of the panicking goroutine |

A `*PanicError` matches `worker.ErrJobPanic` with `errors.Is`, and unwraps to
the panic value when that value is itself an error.

Panicking jobs are counted in `JobsPanicked` (and `JobsFailed`), and logged at
`Error` level with the stack if a logger is set.

### Panic handler

`WithPanicHandler` is called for every recovered panic, with the job and the
recovered value — a good place to report to an error tracker:

```go
worker.WithPanicHandler(func(job worker.Job, recovered any) {
    sentry.CurrentHub().Recover(recovered)
})
```

A panic inside the panic handler, or inside any [hook](./hooks.md), is also
recovered, so a faulty callback cannot take down a worker.

A panic on a goroutine your job starts itself is **not** recovered — Go only
recovers panics on the goroutine they happen on.
