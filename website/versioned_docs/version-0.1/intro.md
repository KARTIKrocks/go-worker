---
id: intro
title: Overview
sidebar_label: Overview
description: A high-performance, zero-dependency worker pool for Go with retries, panic recovery, graceful shutdown, typed futures, groups, batching, and a cron scheduler.
slug: /
---

# go-worker

A high-performance, zero-dependency worker pool for Go with retries,
scheduling, and observability.

```bash
go get github.com/KARTIKrocks/go-worker
```

## What you get

| | |
| --- | --- |
| **Worker pool** | A fixed number of goroutines pulling from a bounded queue |
| **Retries** | Exponential backoff, a delay cap, jitter, per-attempt timeouts, and permanent errors |
| **Panic recovery** | A panicking job fails with its stack trace; the worker keeps running |
| **Graceful shutdown** | Drain everything on `Close`, or force-cancel after a timeout |
| **Middleware & hooks** | Wrap every attempt, and observe job and worker lifecycles |
| **Typed futures** | Generic async results with `SubmitTyped` and `Future[T]` |
| **Groups & batches** | Fan-out/fan-in with all errors or cancel-on-first-error, and chunked bulk work |
| **Scheduler** | Cron expressions, intervals, daily times, and one-shot tasks |
| **Flow control** | Ticker, debouncer, throttler, and rate limiter |
| **Metrics** | Atomic counters, success rate, and throughput |
| **Zero dependencies** | Standard library only |

Everything is built on one `Pool`: groups, futures, the scheduler, and the
flow-control helpers all submit their work to a pool you create and own, so
one set of limits, retries, and metrics covers all of it.

## Where to go next

- **[Getting Started](./getting-started.md)** — create a pool and submit work
- **[Worker Pool](./pool.md)** — the ways to submit a job and what each one waits for
- **[Retries and Panics](./retries.md)** — what is retried, when, and how often
- **[Scheduler](./scheduler.md)** — cron and interval tasks
- **[API Reference](https://pkg.go.dev/github.com/KARTIKrocks/go-worker)** —
  full generated godoc on pkg.go.dev

## Documentation layout

These guides explain concepts, patterns, and configuration. For exact type
signatures, method sets, and struct fields, use
[pkg.go.dev](https://pkg.go.dev/github.com/KARTIKrocks/go-worker) — it is
generated from the source and is always authoritative.

## Requirements

Go 1.27 or later.

go-worker is pre-1.0. Minor releases may contain breaking changes; each one is
listed in the
[changelog](https://github.com/KARTIKrocks/go-worker/blob/main/CHANGELOG.md).
