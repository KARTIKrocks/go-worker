---
id: scheduler
title: Scheduler
description: Run recurring and one-off tasks on a go-worker pool — intervals, cron, daily times, overlap policies, jitter, max runs, and runtime control.
---

# Scheduler

The scheduler runs named tasks on a pool at the times their schedules say.
Each run is submitted to the pool, so it gets the pool's retries, timeouts,
middleware, and metrics like any other job.

```go
sched := worker.NewScheduler(pool)

sched.EveryFunc("cleanup", 5*time.Minute, cleanupExpiredSessions)

if err := sched.CronFunc("report", "0 9 * * MON-FRI", sendDailyReport); err != nil {
    log.Fatal(err) // invalid cron expression
}

if err := sched.DailyFunc("backup", []string{"02:00", "14:00"}, backup); err != nil {
    log.Fatal(err) // invalid time
}

sched.OnceFunc("migrate", time.Now().Add(time.Hour), migrate)

sched.Start()
defer sched.Stop()
```

## Adding tasks

| Method | Runs |
| --- | --- |
| `Every` / `EveryFunc(name, interval, ...)` | every `interval` |
| `Cron` / `CronFunc(name, expr, ...)` | on a [cron expression](./schedules.md#cron-expressions); returns a parse error |
| `Daily` / `DailyFunc(name, times, ...)` | at `"HH:MM"` times each day; returns a parse error |
| `Once` / `OnceFunc(name, at, ...)` | once, at `at` |
| `At(name, start, interval, job, ...)` | at `start`, then every `interval` |
| `Schedule` / `ScheduleFunc(name, schedule, ...)` | on any [`Schedule`](./schedules.md#custom-schedules) |

Methods taking a `Job` have a `Func` variant taking a function. The methods
that cannot fail return the scheduler, so they chain:

```go
sched.
    EveryFunc("heartbeat", 30*time.Second, heartbeat).
    EveryFunc("refresh-cache", 10*time.Minute, refreshCache)
```

Task names are unique. Adding a task under an existing name **replaces** it
and cancels the old task's in-flight runs.

Tasks can be added before or after `Start`.

## Task options

| Option | Effect |
| --- | --- |
| `WithOverlapPolicy(p)` | what to do when a run is due while the previous one is still running — default `OverlapSkip` |
| `WithJitter(d)` | delay each run by a random amount up to `d` |
| `WithMaxRuns(n)` | stop after `n` runs; `0` means unlimited |
| `WithRunImmediate()` | also run as soon as the scheduler starts |

```go
sched.EveryFunc("sync", time.Hour, syncInventory,
    worker.WithRunImmediate(),
    worker.WithJitter(5*time.Minute),
    worker.WithOverlapPolicy(worker.OverlapSkip),
)
```

### Overlap policies

| Policy | When a run is due and the previous one is still running |
| --- | --- |
| `OverlapSkip` | skip this run (the default) |
| `OverlapAllow` | start it anyway, so runs overlap |
| `OverlapQueue` | run it as soon as the current one finishes; up to 100 queued runs per task |

`OverlapSkip` is the safe default for jobs that should never run twice at once,
such as a sync that is not idempotent.

### Jitter

When many processes run the same schedule — every replica of a service
refreshing a cache on the hour — jitter spreads them out so they do not all hit
the backend in the same second.

### Run immediately

`WithRunImmediate` fires the task once when the scheduler starts, then follows
its schedule. For a `Once` task, the immediate run replaces the scheduled one.

## Controlling tasks at runtime

```go
sched.Pause("sync")   // skip runs until resumed; false if no such task
sched.Resume("sync")
sched.Trigger("sync") // run now, outside the schedule
sched.Remove("sync")  // delete the task and cancel its in-flight runs
sched.Tasks()         // names of every task
```

A paused task keeps its due slot, so on `Resume` it runs once promptly instead
of waiting a full period.

`Trigger` follows the same rules as a scheduled run: it respects the pause
state, `WithMaxRuns`, and the overlap policy. It returns `true` if a run was
started or queued, and `false` if not — including when no task has that name.

## Inspecting tasks

```go
info := sched.TaskInfo("report") // nil if there is no such task
fmt.Println(info.Next, info.LastRun, info.LastError, info.SuccessRate())
```

| Field | Meaning |
| --- | --- |
| `Name` | task name |
| `Next` | when it is next due |
| `Running` | whether a run is in progress |
| `Paused` | whether it is paused |
| `RunCount`, `MaxRuns` | runs started, and the `WithMaxRuns` limit |
| `LastRun`, `LastDuration`, `LastError` | the most recent finished run |
| `TotalRuns`, `TotalErrors` | finished runs, and how many failed |

`SuccessRate()` is the fraction of finished runs that succeeded, or `0` before
the first one finishes.

## Scheduler options

```go
sched := worker.NewScheduler(pool,
    worker.WithSchedulerLogger(slog.Default()),
    worker.WithTickInterval(time.Second),
    worker.WithOnTaskStart(func(name string) { /* ... */ }),
    worker.WithOnTaskEnd(func(name string, err error, d time.Duration) { /* ... */ }),
)
```

| Option | Default | Effect |
| --- | --- | --- |
| `WithSchedulerLogger(l)` | none | logs start, stop, and failed runs |
| `WithTickInterval(d)` | `100ms` | how often due tasks are checked — the scheduler's precision |
| `WithOnTaskStart(fn)` | none | called when a run starts |
| `WithOnTaskEnd(fn)` | none | called when a run finishes, with its error and duration |

The task callbacks are recovered if they panic.

## How runs execute

Each run calls `SubmitJobWait` on the pool. A run therefore:

- **occupies a pool worker** for its duration, and waits for queue space if the
  pool is full — a busy pool delays scheduled work,
- gets the pool's **retries and job timeout**; a run "finishes" only after its
  last attempt,
- runs with a context that is **cancelled** when the task is removed or
  replaced, or the scheduler stops.

A run's time is measured from submission to completion, so `LastDuration`
includes time spent waiting in the queue.

## Stopping

`Stop` ends the scheduling loop and cancels the context of in-flight runs. It
does not wait for them to return; close the pool afterwards to do that.

`Stop` is **permanent**: `Start` after `Stop` does nothing. Create a new
scheduler instead. `Start` is idempotent, and `IsRunning()` reports whether the
loop is active.
