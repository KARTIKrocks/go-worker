package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Ticker submits a job at a fixed interval.
type Ticker struct {
	pool     *Pool
	job      Job
	interval time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	runFirst bool
	started  atomic.Bool
}

// NewTicker creates a ticker that submits job every interval.
// It panics if interval <= 0.
func NewTicker(pool *Pool, interval time.Duration, job Job) *Ticker {
	if interval <= 0 {
		panic(fmt.Sprintf("worker: NewTicker: interval must be > 0, got %v", interval))
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Ticker{pool: pool, job: job, interval: interval, ctx: ctx, cancel: cancel}
}

// NewTickerFunc creates a ticker from a function.
func NewTickerFunc(pool *Pool, interval time.Duration, fn func(context.Context) error) *Ticker {
	return NewTicker(pool, interval, JobFunc(fn))
}

// NewTickerImmediate creates a ticker that fires immediately, then at interval.
func NewTickerImmediate(pool *Pool, interval time.Duration, job Job) *Ticker {
	t := NewTicker(pool, interval, job)
	t.runFirst = true
	return t
}

// Start begins the ticker loop. It is idempotent.
func (t *Ticker) Start() {
	if t.started.Swap(true) {
		return
	}
	t.wg.Add(1)
	go t.run()
}

func (t *Ticker) run() {
	defer t.wg.Done()

	if t.runFirst {
		t.submit()
	}

	ticker := time.NewTicker(t.interval)
	defer ticker.Stop()

	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.submit()
		}
	}
}

// submit enqueues the job. Waiting for queue space is bounded by the ticker's
// context so Stop is never blocked behind a full or paused pool, but the job
// itself runs detached from it: stopping the ticker does not cancel runs
// that were already submitted.
func (t *Ticker) submit() {
	_ = t.pool.submit(t.ctx, context.Background(), t.job, nil, true)
}

// Stop stops the ticker and waits for the goroutine to exit. Runs already
// submitted to the pool are not cancelled.
func (t *Ticker) Stop() {
	t.cancel()
	t.wg.Wait()
}
