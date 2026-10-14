---
id: getting-started
title: Getting Started
description: Install go-worker, create a pool, submit fire-and-forget and awaited jobs, and shut the pool down cleanly.
---

# Getting Started

## Installation

Requires **Go 1.27+**.

```bash
go get github.com/KARTIKrocks/go-worker
```

## A first pool

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/KARTIKrocks/go-worker"
)

func main() {
    pool, err := worker.NewPool(
        worker.WithWorkers(8),               // 8 goroutines process jobs
        worker.WithQueueSize(500),           // up to 500 jobs wait in line
        worker.WithMaxRetries(3),            // retry a failed job up to 3 times
        worker.WithJobTimeout(time.Minute),  // each attempt gets a minute
    )
    if err != nil {
        log.Fatal(err)
    }
    defer pool.Close() // waits for queued and running jobs to finish

    // Fire and forget.
    pool.Submit(func(ctx context.Context) error {
        fmt.Println("hello from a worker")
        return nil
    })

    // Submit and wait for the result.
    err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
        return sendEmail(ctx)
    })
    if err != nil {
        log.Printf("email failed after retries: %v", err)
    }
}
```

A few things to notice:

- **Jobs receive a context.** It is cancelled when the attempt times out, when
  the caller of `SubmitWait` gives up, or when the pool is force-closed.
  Long-running jobs should watch it.
- **Jobs return an error.** A non-nil error marks the job as failed and, with
  `WithMaxRetries`, triggers a retry.
- **`Close` drains.** It stops accepting new jobs and waits for everything
  already queued to run. See [Lifecycle](./lifecycle.md) for bounded
  shutdown.

## Typed results

When a job produces a value, `SubmitTyped` returns a typed future:

```go
future := worker.SubmitTyped(pool, func(ctx context.Context) (*User, error) {
    return db.GetUser(ctx, id)
})

user, err := future.Await(ctx)
```

## Fan-out

`ErrorGroup` runs several jobs on the pool and cancels the rest as soon as one
fails, like `errgroup.Group`:

```go
g := worker.NewErrorGroupContext(ctx, pool)

g.Go(func(ctx context.Context) error { return fetchProfile(ctx) })
g.Go(func(ctx context.Context) error { return fetchOrders(ctx) })
g.Go(func(ctx context.Context) error { return fetchInvoices(ctx) })

if err := g.Wait(); err != nil {
    return err
}
```

## Next steps

- [Worker Pool](./pool.md) — every way to submit a job
- [Configuration](./configuration.md) — every option and its default
- [Retries and Panics](./retries.md) — how failures are handled
- [Scheduler](./scheduler.md) — recurring and cron tasks

Runnable programs are in
[`examples/`](https://github.com/KARTIKrocks/go-worker/tree/main/examples) —
basic, retries, middleware, future, errorgroup, batch, scheduler,
rate-limiter, and graceful-shutdown.
