---
id: groups
title: Groups
description: Fan-out and fan-in on a go-worker pool — Group collects every error, ErrorGroup cancels the rest on the first error, like errgroup.
---

# Groups

Groups run several functions on a pool and wait for all of them. There are two,
differing only in what happens when a function fails.

| | `Group` | `ErrorGroup` |
| --- | --- | --- |
| On the first error | keeps going | cancels the group's context |
| Wait returns | every error (`WaitAll`) or the first (`Wait`) | the first error |
| Modeled on | — | `golang.org/x/sync/errgroup` |

Unlike `errgroup`, the functions run on the pool's workers, so the pool's
concurrency limit, [job timeout](./configuration.md#job-timeout),
[retries](./retries.md), [middleware](./middleware.md), and
[metrics](./metrics.md) all apply to them.

## ErrorGroup

Use it when the results only matter together — loading the pieces of one
response, for instance:

```go
g := worker.NewErrorGroupContext(r.Context(), pool)

var profile *Profile
var orders []Order

g.Go(func(ctx context.Context) error {
    var err error
    profile, err = loadProfile(ctx, userID)
    return err
})
g.Go(func(ctx context.Context) error {
    var err error
    orders, err = loadOrders(ctx, userID)
    return err
})

if err := g.Wait(); err != nil {
    return err
}
```

When a function returns an error, the group's context is cancelled. Functions
still running see `ctx.Done()`, and functions that have not started yet are
skipped. `Wait` returns that first error.

`Context()` returns the group's context — cancelled on the first error, or when
`Wait` returns — for work outside the group that should stop with it.

## Group

Use it when every task should run regardless of the others, and you want all
the failures:

```go
g := worker.NewGroup(pool)

for _, id := range accountIDs {
    g.Go(func(ctx context.Context) error {
        return reconcile(ctx, id)
    })
}

for _, err := range g.WaitAll() {
    slog.Error("reconcile failed", "err", err)
}
```

`WaitAll` returns the errors in the order the functions finished. `Wait`
returns just the first of them, or `nil`.

`NewGroupContext(ctx, pool)` ties the group to a parent context. If it is
cancelled, functions that have not started are skipped, and each records
`context.Canceled` in `WaitAll`.

## Things to know

**`Go` can block.** It submits to the pool, so if the queue is full it waits
for space — or until the group's context ends. Launching thousands of tasks
from one loop is fine, but the loop proceeds at the pool's pace.

**Don't wait from inside the same pool.** A job that creates a group on its own
pool and calls `Wait` holds a worker while it waits for other workers. If
every worker does that, nothing is left to run the group's tasks and the pool
deadlocks. Use a separate pool for nested fan-out.

**Use a group once.** `Wait` cancels the group's context when it returns, so
functions added after `Wait` are skipped. Create a new group for each batch of
work.

**Retries are per function.** With `WithMaxRetries`, each function is retried
on its own before its result reaches the group. In an `ErrorGroup`, the group
is only cancelled once a function has failed for good.
