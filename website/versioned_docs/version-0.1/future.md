---
id: future
title: Future
description: Typed asynchronous results with SubmitTyped and Future[T] — awaiting with a context, polling with Done, and how retries and panics resolve a future.
---

# Future

`SubmitTyped` runs a function that returns a value and gives you a typed
`*Future[T]` for its result:

```go
future := worker.SubmitTyped(pool, func(ctx context.Context) (Report, error) {
    return buildReport(ctx, month)
})

// ...do other work...

report, err := future.Await(ctx)
```

The type parameter is inferred from the function, so there is no need to
write `SubmitTyped[Report]`.

## Awaiting

`Await` blocks until the result is ready or `ctx` ends. If `ctx` ends first, it
returns the zero value and `ctx.Err()` — and the job **keeps running**, since
the future is not tied to the caller's context. A later `Await` can still
collect the result.

`Done` reports, without blocking, whether the result is ready:

```go
if future.Done() {
    report, err := future.Await(context.Background()) // returns immediately
}
```

`Await` can be called any number of times, from any number of goroutines, and
always returns the same result.

## Running several at once

Futures are the simplest way to run a few independent calls in parallel and
collect typed results:

```go
userF := worker.SubmitTyped(pool, func(ctx context.Context) (*User, error) {
    return users.Get(ctx, id)
})
ordersF := worker.SubmitTyped(pool, func(ctx context.Context) ([]Order, error) {
    return orders.ForUser(ctx, id)
})

user, err := userF.Await(ctx)
if err != nil {
    return err
}
orders, err := ordersF.Await(ctx)
if err != nil {
    return err
}
```

If one failure should cancel the others, use an
[`ErrorGroup`](./groups.md#errorgroup) instead.

## How the result is decided

| Outcome | `Await` returns |
| --- | --- |
| success | the value, `nil` |
| failure after all retries | the value from the **final** attempt, its error |
| panic | the zero value, a `*PanicError` (matches `ErrJobPanic`) |
| pool closed before it could be queued | the zero value, `ErrPoolClosed` |
| dropped by `CloseWithTimeout` | the zero value, `ErrPoolClosed` |

With [retries](./retries.md) enabled, earlier attempts' values are discarded;
only the attempt that decides the job's outcome sets the result.

## Submitting

`SubmitTyped` submits like `Submit`: it **waits for queue space** if the queue
is full, and the job runs with its own context, independent of the caller's.
There is no non-blocking variant. Checking `pool.QueueLength()` first is only a
hint, since other submitters can fill the queue in between; for real load
shedding, use `TrySubmit` with a job that sends its result on a channel.
