# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
