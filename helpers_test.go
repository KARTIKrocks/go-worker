package worker

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

// recorder collects the names of jobs that ran, in order.
type recorder struct {
	mu  sync.Mutex
	ran []string
}

func (r *recorder) job(name string) Job {
	return JobFunc(func(context.Context) error {
		r.mu.Lock()
		r.ran = append(r.ran, name)
		r.mu.Unlock()
		return nil
	})
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.ran)
}

func (r *recorder) expect(t *testing.T, want ...string) {
	t.Helper()
	synctest.Wait()
	if got := r.got(); !slices.Equal(got, want) {
		t.Fatalf("ran %v, want %v", got, want)
	}
}

// newTestPool returns a single-worker pool so jobs run in submission order.
func newTestPool(t *testing.T) *Pool {
	t.Helper()
	p, err := NewPool(WithWorkers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// ---------- Debouncer ----------

func TestDebouncer_TrailingRunsLastOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		d := NewDebouncer(newTestPool(t), 50*time.Millisecond)
		for _, name := range []string{"a", "b", "c"} {
			d.Submit(r.job(name))
			time.Sleep(10 * time.Millisecond)
		}
		r.expect(t)
		time.Sleep(50 * time.Millisecond)
		r.expect(t, "c")
		time.Sleep(time.Second)
		r.expect(t, "c")
	})
}

func TestDebouncer_StaleTimerIgnored(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		d := NewDebouncer(newTestPool(t), time.Hour)
		d.Submit(r.job("a"))
		stale := d.gen
		d.Submit(r.job("b"))

		// Simulates a timer that fired just before Submit stopped it.
		d.fire(stale)
		r.expect(t)

		d.Cancel()
		d.fire(stale + 1) // the "b" timer racing Cancel
		r.expect(t)
	})
}

func TestDebouncer_LeadingIgnoresWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		d := NewDebouncerLeading(newTestPool(t), 50*time.Millisecond)
		d.Submit(r.job("a"))
		d.Submit(r.job("b"))
		d.Submit(r.job("c"))
		r.expect(t, "a")

		// Flush must not re-run the job that already fired.
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		r.expect(t, "a")

		d.Submit(r.job("d"))
		time.Sleep(10 * time.Millisecond)
		d.Submit(r.job("e"))
		time.Sleep(100 * time.Millisecond)
		r.expect(t, "a", "d")

		d.Submit(r.job("f"))
		r.expect(t, "a", "d", "f")
	})
}

func TestDebouncer_Flush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		p := newTestPool(t)
		d := NewDebouncer(p, 50*time.Millisecond)
		d.Submit(r.job("a"))
		if err := d.Flush(); err != nil {
			t.Fatal(err)
		}
		r.expect(t, "a")
		time.Sleep(time.Second)
		r.expect(t, "a")

		_ = p.Close()
		d.Submit(r.job("b"))
		if err := d.Flush(); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("Flush on closed pool: got %v, want ErrPoolClosed", err)
		}
	})
}

// ---------- Throttler ----------

func TestThrottler_Submit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		p := newTestPool(t)
		th := NewThrottler(p, 100*time.Millisecond)
		if !th.Submit(r.job("a")) {
			t.Fatal("first Submit rejected")
		}
		if th.Submit(r.job("b")) {
			t.Fatal("Submit inside the window accepted")
		}
		time.Sleep(100 * time.Millisecond)
		if !th.Submit(r.job("c")) {
			t.Fatal("Submit after the window rejected")
		}
		r.expect(t, "a", "c")

		_ = p.Close()
		time.Sleep(100 * time.Millisecond)
		if th.Submit(r.job("d")) {
			t.Fatal("Submit on a closed pool reported success")
		}
		if err := th.Force(r.job("e")); !errors.Is(err, ErrPoolClosed) {
			t.Fatalf("Force on closed pool: got %v, want ErrPoolClosed", err)
		}
	})
}

func TestThrottler_Trailing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		th := NewThrottler(newTestPool(t), 100*time.Millisecond)
		th.SubmitTrailing(r.job("a")) // window open: runs now
		th.SubmitTrailing(r.job("b"))
		th.SubmitTrailing(r.job("c")) // replaces b
		r.expect(t, "a")
		time.Sleep(100 * time.Millisecond)
		r.expect(t, "a", "c")
	})
}

func TestThrottler_ResetDropsStaleTrailing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		th := NewThrottler(newTestPool(t), time.Hour)
		th.SubmitTrailing(r.job("a"))
		th.SubmitTrailing(r.job("b")) // pending trailing
		stale := th.gen
		th.Reset()
		th.SubmitTrailing(r.job("c")) // window open after Reset: runs now
		th.SubmitTrailing(r.job("d")) // new pending trailing

		// Simulates the pre-Reset timer firing late.
		th.fireTrailing(stale)
		r.expect(t, "a", "c")
	})
}

// ---------- RateLimiter ----------

func TestRateLimiter_TrySubmit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var r recorder
		rl := NewRateLimiter(newTestPool(t), 2, 100*time.Millisecond)
		defer rl.Stop()
		for _, name := range []string{"a", "b"} {
			if err := rl.TrySubmit(r.job(name)); err != nil {
				t.Fatal(err)
			}
		}
		if err := rl.TrySubmit(r.job("c")); !errors.Is(err, ErrRateLimited) {
			t.Fatalf("got %v, want ErrRateLimited", err)
		}
		time.Sleep(100 * time.Millisecond)
		synctest.Wait() // let the refill goroutine run
		if err := rl.TrySubmit(r.job("d")); err != nil {
			t.Fatalf("after refill: %v", err)
		}
		r.expect(t, "a", "b", "d")
	})
}

func TestRateLimiter_ReturnsTokenWhenPoolRejects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1), WithQueueSize(0))
		defer p.Close()
		block := make(chan struct{})
		_ = p.Submit(func(context.Context) error { <-block; return nil })
		synctest.Wait()

		rl := NewRateLimiter(p, 1, time.Hour)
		defer rl.Stop()
		if err := rl.TrySubmit(JobFunc(func(context.Context) error { return nil })); !errors.Is(err, ErrPoolFull) {
			t.Fatalf("got %v, want ErrPoolFull", err)
		}
		close(block)
		synctest.Wait()
		if err := rl.TrySubmit(JobFunc(func(context.Context) error { return nil })); err != nil {
			t.Fatalf("token was not returned: %v", err)
		}
	})
}

func TestRateLimiter_Stopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rl := NewRateLimiter(newTestPool(t), 5, time.Second)
		rl.Stop()
		job := JobFunc(func(context.Context) error { return nil })
		if err := rl.Submit(context.Background(), job); !errors.Is(err, ErrLimiterStopped) {
			t.Fatalf("Submit: got %v, want ErrLimiterStopped", err)
		}
		if err := rl.TrySubmit(job); !errors.Is(err, ErrLimiterStopped) {
			t.Fatalf("TrySubmit: got %v, want ErrLimiterStopped", err)
		}
	})
}

// ---------- Constructor validation ----------

func TestHelpers_InvalidArgsPanic(t *testing.T) {
	p := newTestPool(t)
	job := JobFunc(func(context.Context) error { return nil })
	for name, fn := range map[string]func(){
		"NewTicker zero interval":      func() { NewTicker(p, 0, job) },
		"NewRateLimiter zero n":        func() { NewRateLimiter(p, 0, time.Second) },
		"NewRateLimiter zero interval": func() { NewRateLimiter(p, 1, 0) },
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected panic")
				}
			}()
			fn()
		})
	}
}

func TestThrottler_NewWindowDropsStaleTrailing(t *testing.T) {
	for name, open := range map[string]func(*Throttler, Job){
		"SubmitTrailing": func(th *Throttler, j Job) {
			th.mu.Lock()
			th.lastRun = time.Time{} // window reopened while the timer callback was delayed
			th.mu.Unlock()
			th.SubmitTrailing(j)
		},
		"Force": func(th *Throttler, j Job) { _ = th.Force(j) },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var r recorder
				th := NewThrottler(newTestPool(t), time.Hour)
				th.SubmitTrailing(r.job("a"))
				th.SubmitTrailing(r.job("b")) // pending trailing
				stale := th.gen
				open(th, r.job("c"))

				// The "b" timer fires late, after the new window opened.
				th.fireTrailing(stale)
				r.expect(t, "a", "c")
			})
		})
	}
}
