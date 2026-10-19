---
id: middleware
title: Middleware
description: Wrap every job attempt in a go-worker pool with middleware for logging, tracing, metrics, or context values, and how ordering and retries interact.
---

# Middleware

Middleware wraps job execution, the same way HTTP middleware wraps a handler:

```go
type Middleware func(next func(ctx context.Context) error) func(ctx context.Context) error
```

Use it for concerns that apply to every job — timing, logging, tracing,
injecting values into the context.

```go
func timing(next func(context.Context) error) func(context.Context) error {
    return func(ctx context.Context) error {
        start := time.Now()
        err := next(ctx)
        slog.Info("job finished", "took", time.Since(start), "err", err)
        return err
    }
}

pool, _ := worker.NewPool(worker.WithMiddleware(timing))
```

## Order

`WithMiddleware` appends, and the **first** middleware added is the
**outermost**:

```go
worker.WithMiddleware(tracing, logging, recoverAndTag)
```

runs as `tracing(logging(recoverAndTag(job)))`. Put middleware that should see
everything — tracing spans — first.

## Middleware runs per attempt

The chain wraps **each attempt**, not the job as a whole. With retries, a
logging middleware logs every attempt; a tracing middleware opens a span per
attempt, which is usually what you want for seeing retries in a trace.

Each attempt's context already carries the [job timeout](./configuration.md#job-timeout),
so it is cancelled when the timeout expires. That only stops `next` if the
job honours its context.

For something that should happen once per job — counting jobs, timing the
whole job including retries — use [hooks](./hooks.md) instead.

## Examples

### Tracing

```go
func tracing(next func(context.Context) error) func(context.Context) error {
    return func(ctx context.Context) error {
        ctx, span := tracer.Start(ctx, "worker.job")
        defer span.End()

        err := next(ctx)
        if err != nil {
            span.RecordError(err)
        }
        return err
    }
}
```

### Turning a class of errors permanent

Middleware can change the job's result. This one stops retries for any
validation error, without every job having to remember to wrap it:

```go
func validationIsPermanent(next func(context.Context) error) func(context.Context) error {
    return func(ctx context.Context) error {
        err := next(ctx)
        var verr *ValidationError
        if errors.As(err, &verr) {
            return worker.Permanent(err)
        }
        return err
    }
}
```

## Middleware and panics

A panic in a job propagates through the middleware on its way to the pool's
recovery, so a middleware's code after `next` does not run for a panicking
job. Use `defer` for anything that must always happen, such as ending a span.
