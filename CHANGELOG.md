# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ErrRateLimited` and `ErrLimiterStopped` sentinel errors for `RateLimiter`
- `Debouncer.Flush` and `Throttler.Force` return the submit error
- `Permanent(err)` marks an error as not retryable
- `WithRetryJitter(fraction)` randomizes retry delays
- `PanicError` carries the panic value and stack trace; it matches `ErrJobPanic`
- Cron: step ranges (`0-30/10`, `5/20`), month and weekday names (`JAN`, `MON-FRI`), `7` for Sunday, and the macros `@yearly`, `@annually`, `@monthly`, `@weekly`, `@daily`, `@midnight`, `@hourly`

### Fixed

- `DailySchedule` fired an hour early or late on daylight-saving change days; a time skipped by the clock change (02:30 when clocks jump 02:00→03:00) now fires after the jump (03:30)
- `CronSchedule.Next` looped forever for times inside a spring-forward gap (e.g. `0 2 * * *` on a US DST day), hanging the scheduler; such times are now skipped that day
- A `Once` task with `WithRunImmediate` ran twice (immediately and at its scheduled time)
- A panic in `WithOnTaskStart`/`WithOnTaskEnd` callbacks crashed the process; it is now recovered and logged
- `Start` after `Stop` reported `IsRunning() == true` without running anything
- Cron treated a day field such as `*/2` as restricted, applying OR instead of AND with the other day field
- A scheduled run and a `Trigger` could both start a run under `OverlapSkip`, or exceed `WithMaxRuns`
- Retry backoff could overflow into a negative delay with large delays and many retries; it now saturates
- A panic in a hook or in the panic handler crashed the process; it is now recovered and logged
- `WithMaxRetryDelay` accepted negative values
- A panic or `Permanent` error returned as the job's context was cancelled was reported as `context.Canceled`
- `JobsRetried` counted retries that were cancelled during the backoff delay and never ran
- `Debouncer`: a timer callback that had already fired could run after `Cancel`/`Flush` or a newer `Submit` and submit a job early
- `Debouncer`: `Flush` in leading mode re-submitted the job that had already run
- `Throttler`: a late trailing timer could submit a stale job after `Reset`, or after `Submit`/`SubmitTrailing`/`Force` opened a new window; opening a window now drops the pending trailing job
- `Debouncer` and `Throttler` no longer hold their lock while blocking on a full queue
- `Throttler.Submit` returned `true` even when the pool rejected the job
- `RateLimiter` lost a token whenever the pool rejected a job
- `NewRateLimiter` with `n <= 0` blocked forever or panicked obscurely; `NewTicker` and `NewRateLimiter` now panic with a clear message on invalid arguments

- `Close` could panic with "send on closed channel" when a submitter was blocked on a full queue
- `SubmitTyped` with retries deadlocked the worker; the `Future` now resolves with the final attempt's result
- `Future.Await` hung forever when the function panicked; it now returns an error wrapping `ErrJobPanic`
- `Group`/`ErrorGroup`/`Batch` with retries called `wg.Done` once per attempt, making `Wait` return early
- `Group`/`ErrorGroup` swallowed panics; they are now reported as errors wrapping `ErrJobPanic`
- `Group`/`ErrorGroup`/`Batch` functions now receive the job context, so `JobTimeout` and forced shutdown apply
- `CloseWithTimeout` dropped queued jobs without notifying waiters; `SubmitWait`, `Future` and groups now get `ErrPoolClosed`
- `Pause` during `Close` hung shutdown; `Pause` is now a no-op once closing
- `Pause` did not stop idle workers, which would still pick up the next submitted job
- Jobs cancelled during a retry delay were not counted in metrics or passed to `OnJobFailed`
- `FixedTimeSchedule` with a zero interval panicked with divide-by-zero; zero/negative intervals no longer fire repeatedly
- `OverlapQueue` queued a run on every scheduler tick instead of once per missed slot
- Scheduler leaked a context per task run; each task now has one context, cancelled by `Remove`, re-`Schedule`, or `Stop`
- `Once` tasks with `WithJitter` ran on every tick after their first run
- Data race between `Scheduler.TaskInfo` and the scheduler loop
- `Ticker.Stop` blocked when the pool queue was full or paused
- `QueueLength` could go transiently negative; it now reports the number of queued jobs exactly

### Changed

- **Breaking:** `Scheduler.Stop` is permanent; `Start` after `Stop` does nothing
- **Breaking:** `Scheduler.Trigger` respects the task's pause state, `WithMaxRuns` and overlap policy, and returns `false` when no run was started or queued
- `RateLimiter.TrySubmit` returns `ErrRateLimited` (was `ErrPoolFull`) when no token is available, and `ErrLimiterStopped` (was `ErrPoolClosed`) after `Stop`; pool errors are passed through
- Skipped `Group` functions (group already cancelled) now add `context.Canceled` to `WaitAll`
- `Group`/`ErrorGroup` functions are now subject to the pool's `JobTimeout`
- **Breaking:** the default `JobTimeout` is now 0 (no timeout); it was 30s, which silently cancelled long jobs
- **Breaking:** `JobTimeout` now applies to each attempt instead of to all attempts plus retry delays, and a timed-out attempt is retried
- **Breaking:** panicking jobs are no longer retried
- Job panics now return a `*PanicError` (still matches `ErrJobPanic`; the message is unchanged)
- Re-scheduling an existing task name cancels the previous task's in-flight runs
- Per-job pool-cancellation watcher goroutine replaced with `context.AfterFunc`

## [0.0.1] - 2026-02-17

### Added

- Worker pool with configurable goroutine count and bounded job queue
- `Submit`, `SubmitWait`, `TrySubmit`, `SubmitContext` submission methods
- Automatic retries with exponential backoff and configurable `MaxRetryDelay` cap
- Panic recovery — workers survive panics and continue processing
- Composable middleware chain for cross-cutting concerns
- Lifecycle hooks (`OnJobStart`, `OnJobComplete`, `OnJobFailed`, `OnWorkerStart`, `OnWorkerStop`)
- Atomic metrics with `Snapshot()` (submitted, completed, failed, retried, panicked, active, queue length, throughput)
- Pause/Resume for flow control without shutdown
- Graceful shutdown with optional timeout (`Close`, `CloseWithTimeout`)
- `ErrorGroup` — fan-out with cancel-on-first-error (like `errgroup.Group`)
- `Group` — fan-out with full error collection
- Generic `Batch[T]` processor for bulk operations
- Generic `Future[T]` for typed async results
- `Scheduler` with cron expressions, fixed intervals, daily times, and one-shot schedules
- Configurable scheduler tick interval (`WithTickInterval`)
- Overlap policies: `OverlapSkip`, `OverlapAllow`, `OverlapQueue`
- Scheduler runtime control: `Pause`, `Resume`, `Trigger`, `Remove`, `TaskInfo`
- `Ticker`, `Debouncer`, `Throttler`, `RateLimiter` helpers
- Zero external dependencies (stdlib only)
- Full test suite with race detector coverage
- Benchmarks for `Submit`, `SubmitWait`, `TrySubmit`, middleware

[0.0.1]: https://github.com/KARTIKrocks/go-worker/releases/tag/v0.0.1
