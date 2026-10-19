---
id: debouncer
title: Debouncer
description: Collapse bursts of submissions into one job run with go-worker's Debouncer — trailing-edge and leading-edge modes, Cancel, and Flush.
---

# Debouncer

A debouncer collapses a burst of submissions into a single run. Each submit
restarts a quiet-period timer; the job only runs once submissions stop for
`delay`.

Typical uses: re-indexing after a flurry of edits, saving a document after the
user stops typing, rebuilding a cache after a batch of config changes.

```go
debouncer := worker.NewDebouncer(pool, 500*time.Millisecond)

for _, change := range changes {
    apply(change)
    debouncer.SubmitFunc(func(ctx context.Context) error {
        return reindex(ctx)
    })
}
// reindex runs once, 500ms after the last change
```

## Trailing and leading edge

| Constructor | Runs | Of a burst, runs |
| --- | --- | --- |
| `NewDebouncer(pool, delay)` | `delay` after the **last** submit | the last job submitted |
| `NewDebouncerLeading(pool, delay)` | immediately on the **first** submit | the first job submitted |

A leading-edge debouncer ignores further submits until `delay` passes with no
submit at all, then fires again on the next one. It suits "react now, but not
again until things calm down" — sending one alert for a burst of identical
errors, for instance.

## Methods

| Method | Effect |
| --- | --- |
| `Submit(job)` / `SubmitFunc(fn)` | submit with debouncing |
| `Cancel()` | drop the pending job and reset the timer |
| `Flush() error` | submit the pending job now, and return the submit error |

Call `Flush` during shutdown — before closing the pool — so a pending job is not
lost:

```go
if err := debouncer.Flush(); err != nil {
    slog.Error("flush failed", "err", err)
}
pool.Close()
```

In leading mode there is never a pending job; `Flush` just ends the current
quiet period.

## Things to know

- **Errors from timer-fired submits are dropped.** When the quiet period ends,
  the job is submitted from the timer's goroutine; if that fails (the pool is
  closed), the job is lost. `Flush` is the only way to see a submit error.
- **`Cancel` cannot recall a job already on its way.** Once the timer has
  fired, the job is no longer pending and is submitted even if `Cancel` runs at
  the same moment — like `time.Timer.Stop`.
- **Only the job is debounced.** Each submit replaces the pending job, so
  capture the latest state in it, not the first.
