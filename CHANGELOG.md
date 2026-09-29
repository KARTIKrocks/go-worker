# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- `Close` could panic with "send on closed channel" when a submitter was blocked on a full queue
- `SubmitTyped` with retries deadlocked the worker; the `Future` now resolves with the final attempt's result
- `Future.Await` hung forever when the function panicked; it now returns an error wrapping `ErrJobPanic`
- `Group`/`ErrorGroup`/`Batch` with retries called `wg.Done` once per attempt, making `Wait` return early
- `Group`/`ErrorGroup` swallowed panics; they are now reported as errors wrapping `ErrJobPanic`
- `Group`/`ErrorGroup`/`Batch` functions now receive the job context, so `JobTimeout` and forced shutdown apply
- `CloseWithTimeout` dropped queued jobs without notifying waiters; `SubmitWait`, `Future` and groups now get `ErrPoolClosed`
- `Pause` during `Close` hung shutdown; `Pause` is now a no-op once closing
- Jobs cancelled during a retry delay were not counted in metrics or passed to `OnJobFailed`
- `FixedTimeSchedule` with a zero interval panicked with divide-by-zero; zero/negative intervals no longer fire repeatedly
- `OverlapQueue` queued a run on every scheduler tick instead of once per missed slot
- Scheduler leaked a context per task run; each task now has one context, cancelled by `Remove`, re-`Schedule`, or `Stop`
- `Once` tasks with `WithJitter` ran on every tick after their first run
- Data race between `Scheduler.TaskInfo` and the scheduler loop
- `Ticker.Stop` blocked when the pool queue was full or paused
- `QueueLength` could go transiently negative; it now reports the number of queued jobs exactly

### Changed

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
