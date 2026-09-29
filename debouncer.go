package worker

import (
	"context"
	"sync"
	"time"
)

// Debouncer collapses rapid submissions, executing only the last job
// after a quiet period of the configured delay.
//
// Jobs submitted when a timer fires are submitted with [Pool.SubmitJob];
// if that fails (for example, the pool is closed) the job is dropped.
type Debouncer struct {
	pool    *Pool
	delay   time.Duration
	leading bool

	mu      sync.Mutex
	timer   *time.Timer
	gen     uint64 // bumped by Submit, Cancel and Flush; stale timer callbacks see a newer value and do nothing
	pending Job    // trailing mode only
	fired   bool   // leading mode: the current window has already fired
}

// NewDebouncer creates a trailing-edge debouncer.
func NewDebouncer(pool *Pool, delay time.Duration) *Debouncer {
	return &Debouncer{pool: pool, delay: delay}
}

// NewDebouncerLeading creates a leading-edge debouncer that fires immediately
// on the first call, then ignores subsequent calls until delay has passed
// without a call.
func NewDebouncerLeading(pool *Pool, delay time.Duration) *Debouncer {
	return &Debouncer{pool: pool, delay: delay, leading: true}
}

// Submit submits a job with debouncing.
func (d *Debouncer) Submit(job Job) {
	d.mu.Lock()
	var now Job
	if d.leading {
		if !d.fired {
			d.fired = true
			now = job
		}
	} else {
		d.pending = job
	}
	d.gen++
	gen := d.gen
	if d.timer != nil {
		d.timer.Stop()
	}
	d.timer = time.AfterFunc(d.delay, func() { d.fire(gen) })
	d.mu.Unlock()

	if now != nil {
		_ = d.pool.SubmitJob(now)
	}
}

// fire ends the quiet period started by the Submit that produced gen.
// Stopping a timer does not stop a callback that has already started, so a
// callback from a superseded Submit, or one racing Cancel or Flush, must
// do nothing.
func (d *Debouncer) fire(gen uint64) {
	d.mu.Lock()
	if gen != d.gen {
		d.mu.Unlock()
		return
	}
	job := d.pending
	d.pending, d.fired, d.timer = nil, false, nil
	d.mu.Unlock()

	if job != nil {
		_ = d.pool.SubmitJob(job)
	}
}

// SubmitFunc submits a function with debouncing.
func (d *Debouncer) SubmitFunc(fn func(context.Context) error) {
	d.Submit(JobFunc(fn))
}

// Cancel cancels the pending debounced job.
func (d *Debouncer) Cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reset()
}

// Flush immediately submits the pending job, if any, and returns the submit
// error. In leading mode there is never a pending job; Flush only ends the
// current window.
func (d *Debouncer) Flush() error {
	d.mu.Lock()
	job := d.pending
	d.reset()
	d.mu.Unlock()

	if job == nil {
		return nil
	}
	return d.pool.SubmitJob(job)
}

// reset stops the timer and clears all state. The caller must hold d.mu.
func (d *Debouncer) reset() {
	d.gen++
	if d.timer != nil {
		d.timer.Stop()
		d.timer = nil
	}
	d.pending = nil
	d.fired = false
}
