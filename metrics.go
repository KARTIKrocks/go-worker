package worker

import (
	"sync/atomic"
	"time"
)

// Metrics holds atomic counters for pool statistics.
// Use [Metrics.Snapshot] to get an approximate point-in-time view.
type Metrics struct {
	JobsSubmitted atomic.Int64
	JobsCompleted atomic.Int64
	JobsFailed    atomic.Int64
	JobsRetried   atomic.Int64
	JobsPanicked  atomic.Int64
	ActiveWorkers atomic.Int32
	QueueLength   atomic.Int32
	TotalDuration atomic.Int64 // nanoseconds
	startTime     time.Time    // when the pool was created
}

// Snapshot returns an approximate point-in-time copy of all metrics.
// Individual counters are read atomically, but the snapshot as a whole
// is not taken under a single lock, so values may be slightly inconsistent
// under concurrent load.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		JobsSubmitted: m.JobsSubmitted.Load(),
		JobsCompleted: m.JobsCompleted.Load(),
		JobsFailed:    m.JobsFailed.Load(),
		JobsRetried:   m.JobsRetried.Load(),
		JobsPanicked:  m.JobsPanicked.Load(),
		ActiveWorkers: m.ActiveWorkers.Load(),
		QueueLength:   m.QueueLength.Load(),
		TotalDuration: time.Duration(m.TotalDuration.Load()),
		Elapsed:       time.Since(m.startTime),
	}
}

// MetricsSnapshot is an immutable point-in-time copy of pool metrics.
type MetricsSnapshot struct {
	JobsSubmitted int64
	JobsCompleted int64
	JobsFailed    int64
	JobsRetried   int64
	JobsPanicked  int64
	ActiveWorkers int32
	QueueLength   int32
	TotalDuration time.Duration // sum of all job durations
	Elapsed       time.Duration // wall-clock time since pool creation
}

// AverageJobDuration returns the mean duration of completed jobs.
// Returns 0 if no jobs have completed.
func (s MetricsSnapshot) AverageJobDuration() time.Duration {
	total := s.JobsCompleted + s.JobsFailed
	if total == 0 {
		return 0
	}
	return s.TotalDuration / time.Duration(total)
}

// SuccessRate returns the fraction of jobs that completed without error (0.0–1.0).
// Returns 1.0 if no jobs have been processed.
func (s MetricsSnapshot) SuccessRate() float64 {
	total := s.JobsCompleted + s.JobsFailed
	if total == 0 {
		return 1.0
	}
	return float64(s.JobsCompleted) / float64(total)
}

// Throughput returns the average jobs completed per second of wall-clock time.
// Returns 0 if no jobs have completed or if no time has elapsed.
func (s MetricsSnapshot) Throughput() float64 {
	if s.JobsCompleted == 0 || s.Elapsed == 0 {
		return 0
	}
	return float64(s.JobsCompleted) / s.Elapsed.Seconds()
}
