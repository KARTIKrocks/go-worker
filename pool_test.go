package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

// ---------- Pool basics ----------

func TestNewPool_Defaults(t *testing.T) {
	pool, err := worker.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
}

func TestNewPool_InvalidWorkers(t *testing.T) {
	_, err := worker.NewPool(worker.WithWorkers(0))
	if err == nil {
		t.Fatal("expected error for 0 workers")
	}
	if !errors.Is(err, worker.ErrInvalidConfig) {
		t.Fatalf("expected ErrInvalidConfig, got %v", err)
	}
}

func TestSubmit(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(2), worker.WithQueueSize(10))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var count atomic.Int32

	for range 10 {
		if err := pool.Submit(func(ctx context.Context) error {
			count.Add(1)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	pool.Close()

	if got := count.Load(); got != 10 {
		t.Fatalf("expected 10, got %d", got)
	}
}

func TestSubmitWait(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(2))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sentinel := errors.New("boom")
	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
}

func TestTrySubmit_Full(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, err := worker.NewPool(worker.WithWorkers(1), worker.WithQueueSize(0))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()

		// Block the one worker
		blocker := make(chan struct{})
		_ = pool.Submit(func(ctx context.Context) error {
			<-blocker
			return nil
		})

		// Now queue is 0 and worker is busy
		time.Sleep(10 * time.Millisecond)
		err = pool.TrySubmit(func(ctx context.Context) error { return nil })
		if !errors.Is(err, worker.ErrPoolFull) {
			t.Fatalf("expected ErrPoolFull, got %v", err)
		}
		close(blocker)
	})
}

func TestSubmit_AfterClose(t *testing.T) {
	pool, err := worker.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	err = pool.Submit(func(ctx context.Context) error { return nil })
	if !errors.Is(err, worker.ErrPoolClosed) {
		t.Fatalf("expected ErrPoolClosed, got %v", err)
	}
}

func TestClose_Idempotent(t *testing.T) {
	pool, err := worker.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	pool.Close() // should not panic
}

// ---------- Panic recovery ----------

func TestPanicRecovery(t *testing.T) {
	var caught atomic.Value

	pool, err := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithPanicHandler(func(job worker.Job, r any) {
			caught.Store(r)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		panic("test panic")
	})
	if !errors.Is(err, worker.ErrJobPanic) {
		t.Fatalf("expected ErrJobPanic, got %v", err)
	}
	if caught.Load() != "test panic" {
		t.Fatalf("expected 'test panic', got %v", caught.Load())
	}

	// Pool should still work after panic
	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		return nil
	})
	if err != nil {
		t.Fatalf("expected pool to still work, got %v", err)
	}
}

// ---------- Retries ----------

func TestRetries(t *testing.T) {
	var attempts atomic.Int32

	pool, err := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithMaxRetries(2),
		worker.WithRetryDelay(10*time.Millisecond),
		worker.WithRetryBackoff(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		attempts.Add(1)
		if attempts.Load() < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected success after retries, got %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Fatalf("expected 3 attempts, got %d", got)
	}
}

// ---------- Job timeout ----------

func TestJobTimeout(t *testing.T) {
	pool, err := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithJobTimeout(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

// ---------- Pause / Resume ----------

func TestPauseResume(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, err := worker.NewPool(worker.WithWorkers(2), worker.WithQueueSize(10))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()

		pool.Pause()
		if !pool.IsPaused() {
			t.Fatal("expected paused")
		}

		var ran atomic.Bool
		_ = pool.Submit(func(ctx context.Context) error {
			ran.Store(true)
			return nil
		})

		time.Sleep(50 * time.Millisecond)
		if ran.Load() {
			t.Fatal("job should not have run while paused")
		}

		pool.Resume()
		time.Sleep(50 * time.Millisecond)
		if !ran.Load() {
			t.Fatal("job should have run after resume")
		}
	})
}

// ---------- Metrics ----------

func TestMetrics(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(2))
	if err != nil {
		t.Fatal(err)
	}

	for range 5 {
		_ = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
			return nil
		})
	}
	_ = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		return errors.New("fail")
	})

	pool.Close()
	snap := pool.Snapshot()

	if snap.JobsSubmitted != 6 {
		t.Fatalf("expected 6 submitted, got %d", snap.JobsSubmitted)
	}
	if snap.JobsCompleted != 5 {
		t.Fatalf("expected 5 completed, got %d", snap.JobsCompleted)
	}
	if snap.JobsFailed != 1 {
		t.Fatalf("expected 1 failed, got %d", snap.JobsFailed)
	}
}

// ---------- Middleware ----------

func TestMiddleware(t *testing.T) {
	var order []string
	var mu sync.Mutex

	mw1 := func(next func(ctx context.Context) error) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "mw1-before")
			mu.Unlock()
			err := next(ctx)
			mu.Lock()
			order = append(order, "mw1-after")
			mu.Unlock()
			return err
		}
	}

	mw2 := func(next func(ctx context.Context) error) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			mu.Lock()
			order = append(order, "mw2-before")
			mu.Unlock()
			err := next(ctx)
			mu.Lock()
			order = append(order, "mw2-after")
			mu.Unlock()
			return err
		}
	}

	pool, err := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithMiddleware(mw1, mw2),
	)
	if err != nil {
		t.Fatal(err)
	}

	_ = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		mu.Lock()
		order = append(order, "job")
		mu.Unlock()
		return nil
	})
	pool.Close()

	expected := []string{"mw1-before", "mw2-before", "job", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Fatalf("at %d: expected %q, got %q", i, v, order[i])
		}
	}
}

// ---------- ErrorGroup ----------

func TestErrorGroup_CancelsOnError(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(4))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sentinel := errors.New("first failure")
	g := worker.NewErrorGroup(pool)

	g.Go(func(ctx context.Context) error {
		return sentinel
	})
	g.Go(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})

	if err := g.Wait(); !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
}

func TestGroup_CollectsAllErrors(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(4))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	g := worker.NewGroup(pool)

	for i := range 3 {
		g.Go(func(ctx context.Context) error {
			return fmt.Errorf("error %d", i)
		})
	}

	errs := g.WaitAll()
	if len(errs) != 3 {
		t.Fatalf("expected 3 errors, got %d", len(errs))
	}
}

// ---------- Batch ----------

func TestBatch(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(4))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	var total atomic.Int64

	batch := worker.NewBatch(pool, 3, func(ctx context.Context, items []int) error {
		for _, v := range items {
			total.Add(int64(v))
		}
		return nil
	})

	for i := 1; i <= 10; i++ {
		batch.Add(i)
	}

	if err := batch.Process(context.Background()); err != nil {
		t.Fatal(err)
	}

	if got := total.Load(); got != 55 {
		t.Fatalf("expected 55, got %d", got)
	}
}

// ---------- Future ----------

func TestFuture(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(2))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	f := worker.SubmitTyped(pool, func(ctx context.Context) (int, error) {
		return 42, nil
	})

	val, err := f.Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if val != 42 {
		t.Fatalf("expected 42, got %d", val)
	}
}

func TestFuture_Error(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sentinel := errors.New("compute failed")
	f := worker.SubmitTyped(pool, func(ctx context.Context) (string, error) {
		return "", sentinel
	})

	_, err = f.Await(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
}

// ---------- Scheduler ----------

func TestScheduler_Every(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, err := worker.NewPool(worker.WithWorkers(2))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()

		var count atomic.Int32
		sched := worker.NewScheduler(pool)

		sched.EveryFunc("counter", 100*time.Millisecond, func(ctx context.Context) error {
			count.Add(1)
			return nil
		}, worker.WithRunImmediate())

		sched.Start()
		time.Sleep(500 * time.Millisecond)
		sched.Stop()

		if got := count.Load(); got < 2 {
			t.Fatalf("expected at least 2 runs, got %d", got)
		}
	})
}

func TestScheduler_Remove(t *testing.T) {
	pool, err := worker.NewPool(worker.WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	sched := worker.NewScheduler(pool)
	sched.EveryFunc("test", time.Hour, func(ctx context.Context) error { return nil })

	tasks := sched.Tasks()
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	sched.Remove("test")
	tasks = sched.Tasks()
	if len(tasks) != 0 {
		t.Fatalf("expected 0 tasks, got %d", len(tasks))
	}
}

// ---------- Cron parsing ----------

func TestParseCron_Valid(t *testing.T) {
	cases := []string{
		"*/15 * * * *",
		"0 9 * * 1-5",
		"0 0 1 * *",
		"30 4 1,15 * *",
	}
	for _, expr := range cases {
		s, err := worker.ParseCron(expr)
		if err != nil {
			t.Fatalf("ParseCron(%q) error: %v", expr, err)
		}
		next := s.Next(time.Now())
		if next.IsZero() {
			t.Fatalf("ParseCron(%q) returned zero next time", expr)
		}
	}
}

func TestParseCron_Invalid(t *testing.T) {
	cases := []string{
		"",
		"* *",
		"60 * * * *",
		"* 25 * * *",
	}
	for _, expr := range cases {
		_, err := worker.ParseCron(expr)
		if err == nil {
			t.Fatalf("ParseCron(%q) expected error", expr)
		}
	}
}

// ---------- Hooks ----------

func TestHooks(t *testing.T) {
	var started, completed, failed atomic.Int32

	pool, err := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithHooks(worker.Hooks{
			OnJobStart:    func(j worker.Job) { started.Add(1) },
			OnJobComplete: func(j worker.Job, d time.Duration) { completed.Add(1) },
			OnJobFailed:   func(j worker.Job, e error, d time.Duration) { failed.Add(1) },
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	_ = pool.SubmitWait(context.Background(), func(ctx context.Context) error { return nil })
	_ = pool.SubmitWait(context.Background(), func(ctx context.Context) error { return errors.New("fail") })
	pool.Close()

	if started.Load() != 2 {
		t.Fatalf("expected 2 starts, got %d", started.Load())
	}
	if completed.Load() != 1 {
		t.Fatalf("expected 1 complete, got %d", completed.Load())
	}
	if failed.Load() != 1 {
		t.Fatalf("expected 1 fail, got %d", failed.Load())
	}
}

// ---------- CloseWithTimeout ----------

func TestCloseWithTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pool, err := worker.NewPool(worker.WithWorkers(1), worker.WithJobTimeout(0))
		if err != nil {
			t.Fatal(err)
		}

		_ = pool.Submit(func(ctx context.Context) error {
			<-ctx.Done()
			return nil
		})

		time.Sleep(10 * time.Millisecond)
		start := time.Now()
		pool.CloseWithTimeout(50 * time.Millisecond)
		elapsed := time.Since(start)

		if elapsed > 200*time.Millisecond {
			t.Fatalf("close took too long: %v", elapsed)
		}
	})
}
