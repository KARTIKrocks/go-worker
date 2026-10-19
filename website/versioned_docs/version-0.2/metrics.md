---
id: metrics
title: Metrics
description: Read go-worker pool metrics with Snapshot — submitted, completed, failed, retried and panicked jobs, active workers, queue length, average duration, success rate, and throughput.
---

# Metrics

Every pool keeps atomic counters. `Snapshot` returns a copy of them:

```go
snap := pool.Snapshot()

fmt.Printf("submitted=%d completed=%d failed=%d\n",
    snap.JobsSubmitted, snap.JobsCompleted, snap.JobsFailed)
fmt.Printf("avg=%v success=%.1f%% throughput=%.1f/s\n",
    snap.AverageJobDuration(), snap.SuccessRate()*100, snap.Throughput())
```

## Fields

| Field | Meaning |
| --- | --- |
| `JobsSubmitted` | jobs accepted into the queue |
| `JobsCompleted` | jobs that finished without error |
| `JobsFailed` | jobs that finished with an error, after all retries |
| `JobsRetried` | retry attempts that ran |
| `JobsPanicked` | attempts that panicked |
| `ActiveWorkers` | workers running a job right now |
| `QueueLength` | jobs waiting in the queue right now |
| `TotalDuration` | summed duration of finished jobs, including retries and delays |
| `Elapsed` | time since the pool was created |

`JobsCompleted` and `JobsFailed` count **jobs**, not attempts: a job that fails
twice and then succeeds adds 2 to `JobsRetried` and 1 to `JobsCompleted`. A
panicking job counts in both `JobsPanicked` and `JobsFailed`.

## Derived values

| Method | Returns |
| --- | --- |
| `AverageJobDuration()` | `TotalDuration` divided by finished jobs; `0` if none |
| `SuccessRate()` | completed ÷ finished, from `0.0` to `1.0`; `1.0` if none have finished |
| `Throughput()` | completed jobs per second since the pool was created |

`Throughput` averages over the pool's whole life. For a current rate, take two
snapshots and divide the difference by the time between them.

## Consistency

Each counter is read atomically, but a snapshot is not taken under one lock, so
under load the fields can be very slightly out of step with each other — a job
may count as completed in a snapshot that does not yet count it as submitted.
That is fine for dashboards and alerts; do not build exact accounting on it.

## Exporting to Prometheus

go-worker has no dependencies, so it ships no Prometheus integration. Exporting
a snapshot takes a few lines with `prometheus.NewCounterFunc` and
`prometheus.NewGaugeFunc`:

```go
reg.MustRegister(
    prometheus.NewCounterFunc(prometheus.CounterOpts{
        Name: "worker_jobs_completed_total",
        Help: "Jobs finished without error.",
    }, func() float64 { return float64(pool.Snapshot().JobsCompleted) }),

    prometheus.NewCounterFunc(prometheus.CounterOpts{
        Name: "worker_jobs_failed_total",
        Help: "Jobs finished with an error after all retries.",
    }, func() float64 { return float64(pool.Snapshot().JobsFailed) }),

    prometheus.NewGaugeFunc(prometheus.GaugeOpts{
        Name: "worker_queue_length",
        Help: "Jobs waiting in the queue.",
    }, func() float64 { return float64(pool.QueueLength()) }),

    prometheus.NewGaugeFunc(prometheus.GaugeOpts{
        Name: "worker_active_workers",
        Help: "Workers running a job.",
    }, func() float64 { return float64(pool.ActiveWorkers()) }),
)
```

For per-job latency histograms, observe durations in
[`OnJobComplete` and `OnJobFailed` hooks](./hooks.md).

## Scheduler metrics

Scheduled tasks have their own run counts, last error, and success rate — see
[Inspecting tasks](./scheduler.md#inspecting-tasks).
