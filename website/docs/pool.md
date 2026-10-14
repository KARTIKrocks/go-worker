---
id: pool
title: Worker Pool
description: Submitting work to a go-worker Pool — Submit, SubmitWait, TrySubmit, SubmitContext, the Job interface — and what each submit blocks on.
---

# Worker Pool

A `Pool` runs a fixed number of worker goroutines that take jobs from a
bounded queue. `NewPool` starts the workers immediately:

```go
pool, err := worker.NewPool(worker.WithWorkers(8), worker.WithQueueSize(500))
if err != nil {
    log.Fatal(err) // only invalid configuration fails
}
defer pool.Close()
```

The queue is the pool's backpressure. Once it is full, new submissions either
wait for space or are rejected — you choose which by how you submit.

## Jobs

A job is any `func(ctx context.Context) error`, or any type implementing `Job`:

```go
type Job interface {
    Process(ctx context.Context) error
}
```

Use a function for one-off work. Use the interface when a job carries state,
or when you want [hooks](./hooks.md) and [panic handlers](./retries.md#panics)
to be able to tell jobs apart:

```go
type ResizeImage struct {
    ID    string
    Width int
}

func (j *ResizeImage) Process(ctx context.Context) error {
    return resize(ctx, j.ID, j.Width)
}

pool.SubmitJob(&ResizeImage{ID: "img-42", Width: 800})
```

`JobFunc` adapts a function to `Job` when an API wants the interface:
`worker.JobFunc(fn)`.

## Ways to submit

| Method | When the queue is full | Returns | Job's context |
| --- | --- | --- | --- |
| `Submit(fn)` / `SubmitJob(job)` | waits for space | once enqueued | independent |
| `TrySubmit(fn)` / `TrySubmitJob(job)` | returns `ErrPoolFull` | immediately | independent |
| `SubmitContext(ctx, job)` | waits for space or `ctx` | once enqueued | derived from `ctx` |
| `SubmitWait(ctx, fn)` / `SubmitJobWait(ctx, job)` | waits for space or `ctx` | when the job finishes, with its error | derived from `ctx` |

Every method returns `ErrPoolClosed` once the pool is closing, including a
submit that was already waiting for space.

### Fire and forget

`Submit` returns as soon as the job is queued. Its error only tells you whether
it was queued; the job's own result goes to [hooks](./hooks.md) and
[metrics](./metrics.md):

```go
if err := pool.Submit(sendWelcomeEmail); err != nil {
    // ErrPoolClosed — the pool is shutting down
}
```

### Load shedding

`TrySubmit` never blocks. Use it where waiting is worse than dropping — an
HTTP handler that would rather return `503` than hang:

```go
switch err := pool.TrySubmit(job); {
case errors.Is(err, worker.ErrPoolFull):
    http.Error(w, "busy, try again", http.StatusServiceUnavailable)
    return
case err != nil: // ErrPoolClosed
    http.Error(w, "shutting down", http.StatusServiceUnavailable)
    return
}
```

### Bounded waiting

`SubmitContext` waits for space only as long as `ctx` allows. The job then runs
with a context derived from `ctx`, so **cancelling `ctx` after the job is
queued also cancels the job**. Pass a context whose lifetime matches the job,
not one that ends as soon as the submit returns.

### Waiting for the result

`SubmitWait` queues the job and blocks until it has finished — after all
retries — and returns its final error:

```go
err := pool.SubmitWait(r.Context(), func(ctx context.Context) error {
    return chargeCard(ctx, order)
})
```

If `ctx` ends first, `SubmitWait` returns `ctx.Err()` and the job's context is
cancelled too, so the job can stop early.

## Sizing

**Workers** bound how many jobs run at once. For CPU-bound jobs,
`runtime.GOMAXPROCS(0)` is a sensible start. For I/O-bound jobs, size by what
the downstream system tolerates — the number of connections your database
pool allows, or the request concurrency an API permits.

**Queue size** bounds how much work can pile up. A larger queue absorbs
bigger spikes but holds more work in memory, all of which is lost if the
process dies. A queue size of `0` is allowed: every submit then hands the job
directly to an idle worker, or waits for one.

## Inspecting the pool

```go
pool.QueueLength()   // jobs waiting in the queue
pool.ActiveWorkers() // workers running a job right now
pool.IsPaused()
pool.IsClosed()
pool.Snapshot()      // all counters, see Metrics
```

## Thread safety

Every `Pool` method is safe for concurrent use.
