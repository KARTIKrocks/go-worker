package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Regression tests for concurrency bugs. Internal package so scheduler state
// can be inspected directly.

func TestRegression_CloseWhileSubmitBlocked(t *testing.T) {
	for range 200 {
		p, err := NewPool(WithWorkers(1), WithQueueSize(1))
		if err != nil {
			t.Fatal(err)
		}
		block := make(chan struct{})
		_ = p.Submit(func(context.Context) error { <-block; return nil })
		_ = p.Submit(func(context.Context) error { return nil })

		var wg sync.WaitGroup
		wg.Go(func() {
			// Blocks on the full queue until Close runs.
			_ = p.Submit(func(context.Context) error { return nil })
		})
		time.Sleep(time.Millisecond)
		go func() { time.Sleep(time.Millisecond); close(block) }()
		_ = p.Close()
		wg.Wait()
	}
}

func TestRegression_FutureWithRetries(t *testing.T) {
	p, err := NewPool(WithWorkers(1), WithMaxRetries(2), WithRetryDelay(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var attempts atomic.Int32
	f := SubmitTyped(p, func(context.Context) (int, error) {
		if attempts.Add(1) < 3 {
			return 0, errors.New("transient")
		}
		return 42, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	v, err := f.Await(ctx)
	if err != nil || v != 42 {
		t.Fatalf("got (%d, %v), want (42, nil) from the final attempt", v, err)
	}

	done := make(chan struct{})
	go func() { _ = p.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker stuck after Future job with retries")
	}
}

func TestRegression_FuturePanic(t *testing.T) {
	p, _ := NewPool(WithWorkers(1))
	defer p.Close()
	f := SubmitTyped(p, func(context.Context) (int, error) { panic("boom") })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := f.Await(ctx); !errors.Is(err, ErrJobPanic) {
		t.Fatalf("got %v, want ErrJobPanic", err)
	}
	if !f.Done() {
		t.Fatal("Done() = false after Await returned")
	}
}

func TestRegression_GroupWithRetriesWaitsForAll(t *testing.T) {
	p, _ := NewPool(WithWorkers(2), WithMaxRetries(2), WithRetryDelay(time.Millisecond))
	defer p.Close()
	var slowDone atomic.Bool
	g := NewGroup(p)
	g.Go(func(context.Context) error { time.Sleep(100 * time.Millisecond); slowDone.Store(true); return nil })
	g.Go(func(context.Context) error { return errors.New("x") })
	errs := g.WaitAll()
	if !slowDone.Load() {
		t.Fatal("Wait returned before all jobs finished")
	}
	if len(errs) != 1 {
		t.Fatalf("got %d errors, want 1 (one per job, not per attempt)", len(errs))
	}
	if n := p.Snapshot().JobsPanicked; n != 0 {
		t.Fatalf("JobsPanicked = %d, want 0", n)
	}
}

func TestRegression_GroupPanicReported(t *testing.T) {
	p, _ := NewPool(WithWorkers(2))
	defer p.Close()

	eg := NewErrorGroup(p)
	eg.Go(func(context.Context) error { panic("boom") })
	if err := eg.Wait(); !errors.Is(err, ErrJobPanic) {
		t.Fatalf("ErrorGroup: got %v, want ErrJobPanic", err)
	}

	g := NewGroup(p)
	g.Go(func(context.Context) error { panic("boom") })
	if err := g.Wait(); !errors.Is(err, ErrJobPanic) {
		t.Fatalf("Group: got %v, want ErrJobPanic", err)
	}
}

func TestRegression_GroupRespectsJobTimeout(t *testing.T) {
	p, _ := NewPool(WithWorkers(1), WithJobTimeout(20*time.Millisecond))
	defer p.Close()
	g := NewGroup(p)
	g.Go(func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return nil
		}
	})
	if err := g.Wait(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
}

func TestRegression_CloseTimeoutFailsQueuedWaiters(t *testing.T) {
	p, _ := NewPool(WithWorkers(1), WithQueueSize(10), WithJobTimeout(0))
	_ = p.Submit(func(ctx context.Context) error { <-ctx.Done(); return nil })
	res := make(chan error, 1)
	go func() { res <- p.SubmitWait(context.Background(), func(context.Context) error { return nil }) }()
	time.Sleep(20 * time.Millisecond)
	_ = p.CloseWithTimeout(20 * time.Millisecond)
	select {
	case err := <-res:
		if !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("got %v, want ErrPoolClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("SubmitWait never returned for a job dropped by CloseWithTimeout")
	}
	if n := p.QueueLength(); n != 0 {
		t.Fatalf("QueueLength = %d after close, want 0", n)
	}
}

func TestRegression_PauseDuringClose(t *testing.T) {
	p, _ := NewPool(WithWorkers(1), WithQueueSize(10))
	_ = p.Submit(func(context.Context) error { time.Sleep(30 * time.Millisecond); return nil })
	_ = p.Submit(func(context.Context) error { return nil })
	time.Sleep(5 * time.Millisecond)
	done := make(chan struct{})
	go func() { _ = p.Close(); close(done) }()
	time.Sleep(5 * time.Millisecond)
	p.Pause()
	select {
	case <-done:
	case <-time.After(time.Second):
		p.Resume()
		t.Fatal("Close blocked because Pause was called during shutdown")
	}
}

func TestRegression_RetryCancelRecordsFailure(t *testing.T) {
	var failed atomic.Int32
	p, _ := NewPool(WithWorkers(1), WithMaxRetries(3), WithRetryDelay(time.Second),
		WithJobTimeout(20*time.Millisecond),
		WithHooks(Hooks{OnJobFailed: func(Job, error, time.Duration) { failed.Add(1) }}))
	err := p.SubmitWait(context.Background(), func(context.Context) error { return errors.New("x") })
	_ = p.Close()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want DeadlineExceeded", err)
	}
	if s := p.Snapshot(); s.JobsFailed != 1 || failed.Load() != 1 {
		t.Fatalf("JobsFailed=%d OnJobFailed=%d, want 1 and 1", s.JobsFailed, failed.Load())
	}
}

func TestRegression_ZeroIntervalSchedules(t *testing.T) {
	now := time.Now()
	if got := (&FixedTimeSchedule{Start: now.Add(-time.Hour)}).Next(now); !got.IsZero() {
		t.Fatalf("FixedTimeSchedule zero interval: got %v, want zero", got)
	}
	if got := (&IntervalSchedule{}).Next(now); !got.IsZero() {
		t.Fatalf("IntervalSchedule zero interval: got %v, want zero", got)
	}
}

func TestRegression_OverlapQueueOnePerSlot(t *testing.T) {
	p, _ := NewPool(WithWorkers(4))
	defer p.Close()
	s := NewScheduler(p, WithTickInterval(2*time.Millisecond))
	s.EveryFunc("t", 50*time.Millisecond, func(context.Context) error {
		time.Sleep(200 * time.Millisecond)
		return nil
	}, WithRunImmediate(), WithOverlapPolicy(OverlapQueue))
	s.Start()
	time.Sleep(180 * time.Millisecond)
	s.tasksMu.RLock()
	q := s.tasks["t"].queued.Load()
	s.tasksMu.RUnlock()
	s.Stop()
	// Slots at ~50, 100, 150ms fall inside the first 200ms run.
	if q > 4 {
		t.Fatalf("%d runs queued in 180ms for a 50ms schedule, want one per missed slot", q)
	}
}

func TestRegression_OnceWithJitterRunsOnce(t *testing.T) {
	p, _ := NewPool(WithWorkers(2))
	defer p.Close()
	var runs atomic.Int32
	s := NewScheduler(p, WithTickInterval(2*time.Millisecond))
	s.OnceFunc("t", time.Now().Add(5*time.Millisecond), func(context.Context) error {
		runs.Add(1)
		return nil
	}, WithJitter(time.Millisecond))
	s.Start()
	time.Sleep(100 * time.Millisecond)
	s.Stop()
	if n := runs.Load(); n != 1 {
		t.Fatalf("Once task with jitter ran %d times, want 1", n)
	}
}

func TestRegression_TaskInfoNoRace(t *testing.T) {
	p, _ := NewPool(WithWorkers(2))
	defer p.Close()
	s := NewScheduler(p, WithTickInterval(time.Millisecond))
	s.EveryFunc("t", time.Millisecond, func(context.Context) error { return nil })
	s.Start()
	for range 100 {
		_ = s.TaskInfo("t")
		time.Sleep(100 * time.Microsecond)
	}
	s.Stop()
}

func TestRegression_RemoveCancelsRuns(t *testing.T) {
	p, _ := NewPool(WithWorkers(1))
	defer p.Close()
	cancelled := make(chan struct{})
	s := NewScheduler(p, WithTickInterval(time.Millisecond))
	s.EveryFunc("t", time.Hour, func(ctx context.Context) error {
		<-ctx.Done()
		close(cancelled)
		return ctx.Err()
	}, WithRunImmediate())
	s.Start()
	defer s.Stop()
	time.Sleep(20 * time.Millisecond)
	s.Remove("t")
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("Remove did not cancel the in-flight run")
	}
}

func TestRegression_TickerStopWithFullQueue(t *testing.T) {
	p, _ := NewPool(WithWorkers(1), WithQueueSize(0))
	block := make(chan struct{})
	_ = p.Submit(func(context.Context) error { <-block; return nil })
	tk := NewTickerFunc(p, 5*time.Millisecond, func(context.Context) error { return nil })
	tk.Start()
	time.Sleep(20 * time.Millisecond)
	done := make(chan struct{})
	go func() { tk.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Ticker.Stop blocked behind a full queue")
	}
	close(block)
	_ = p.Close()
}

func TestRegression_TickerStopDoesNotCancelRuns(t *testing.T) {
	p, _ := NewPool(WithWorkers(1))
	started := make(chan struct{})
	var runErr atomic.Value
	tk := NewTickerImmediate(p, time.Hour, JobFunc(func(ctx context.Context) error {
		close(started)
		time.Sleep(30 * time.Millisecond)
		runErr.Store(fmt.Sprint(ctx.Err()))
		return nil
	}))
	tk.Start()
	<-started
	tk.Stop()
	_ = p.Close()
	if got := runErr.Load(); got != "<nil>" {
		t.Fatalf("in-flight ticker run saw ctx.Err() = %v after Stop, want nil", got)
	}
}

func TestRegression_QueueLengthExcludesBlockedSubmitters(t *testing.T) {
	p, _ := NewPool(WithWorkers(1), WithQueueSize(2))
	block := make(chan struct{})
	_ = p.Submit(func(context.Context) error { <-block; return nil })
	time.Sleep(10 * time.Millisecond) // let the worker take it
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() { _ = p.Submit(func(context.Context) error { return nil }) })
	}
	time.Sleep(20 * time.Millisecond)
	if n := p.QueueLength(); n != 2 {
		t.Fatalf("QueueLength = %d with 10 blocked submitters, want 2 (queue capacity)", n)
	}
	if n := p.Snapshot().QueueLength; n != 2 {
		t.Fatalf("Snapshot().QueueLength = %d, want 2", n)
	}
	close(block)
	wg.Wait()
	_ = p.Close()
}

func TestRegression_FutureAsyncMiddlewareNoRace(t *testing.T) {
	// Middleware that runs next on another goroutine and returns on ctx.Done.
	async := func(next func(context.Context) error) func(context.Context) error {
		return func(ctx context.Context) error {
			ch := make(chan error, 1)
			go func() { ch <- next(ctx) }()
			select {
			case err := <-ch:
				return err
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	p, _ := NewPool(WithWorkers(1), WithJobTimeout(10*time.Millisecond), WithMiddleware(async))
	defer p.Close()
	f := SubmitTyped(p, func(context.Context) (int, error) {
		time.Sleep(30 * time.Millisecond) // outlives the job timeout
		return 7, nil
	})
	v, err := f.Await(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) || v != 0 {
		t.Fatalf("got (%d, %v), want (0, DeadlineExceeded)", v, err)
	}
	time.Sleep(40 * time.Millisecond) // let the abandoned attempt finish
	if v, _ := f.Await(context.Background()); v != 0 {
		t.Fatalf("late attempt overwrote resolved value: got %d", v)
	}
}

func TestRegression_OverlapQueueRespectsMaxRuns(t *testing.T) {
	p, _ := NewPool(WithWorkers(2))
	defer p.Close()
	var runs atomic.Int32
	s := NewScheduler(p, WithTickInterval(2*time.Millisecond))
	s.EveryFunc("t", 10*time.Millisecond, func(context.Context) error {
		runs.Add(1)
		time.Sleep(50 * time.Millisecond)
		return nil
	}, WithRunImmediate(), WithOverlapPolicy(OverlapQueue), WithMaxRuns(2))
	s.Start()
	time.Sleep(300 * time.Millisecond)
	s.Stop()
	if n := runs.Load(); n != 2 {
		t.Fatalf("task ran %d times, want 2 (WithMaxRuns)", n)
	}
}

func TestRegression_PauseStopsIdleWorkers(t *testing.T) {
	p, _ := NewPool(WithWorkers(4), WithQueueSize(10))
	defer p.Close()
	time.Sleep(10 * time.Millisecond) // let every worker block waiting for a job
	p.Pause()
	var ran atomic.Bool
	_ = p.Submit(func(context.Context) error { ran.Store(true); return nil })
	time.Sleep(30 * time.Millisecond)
	if ran.Load() {
		t.Fatal("idle worker ran a job while paused")
	}
	if n := p.QueueLength(); n != 1 {
		t.Fatalf("QueueLength = %d while paused, want 1", n)
	}
	p.Resume()
	time.Sleep(30 * time.Millisecond)
	if !ran.Load() {
		t.Fatal("job did not run after Resume")
	}
}
