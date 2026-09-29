package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRetry_TimeoutIsPerAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1), WithJobTimeout(time.Second),
			WithMaxRetries(2), WithRetryDelay(10*time.Second), WithRetryBackoff(false))
		defer p.Close()
		var attempts atomic.Int32
		err := p.SubmitWait(context.Background(), func(ctx context.Context) error {
			if attempts.Add(1) < 3 {
				<-ctx.Done() // this attempt times out
				return ctx.Err()
			}
			return nil
		})
		// Total time is ~22s, far past JobTimeout, yet the third attempt runs.
		if err != nil || attempts.Load() != 3 {
			t.Fatalf("got err=%v attempts=%d, want nil and 3", err, attempts.Load())
		}
	})
}

func TestRetry_NoDefaultJobTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1))
		defer p.Close()
		err := p.SubmitWait(context.Background(), func(ctx context.Context) error {
			time.Sleep(time.Hour)
			return ctx.Err()
		})
		if err != nil {
			t.Fatalf("long job was cancelled by a default timeout: %v", err)
		}
	})
}

func TestRetry_PermanentNotRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1), WithMaxRetries(3), WithRetryDelay(time.Millisecond))
		defer p.Close()
		sentinel := errors.New("bad input")
		var attempts atomic.Int32
		err := p.SubmitWait(context.Background(), func(context.Context) error {
			attempts.Add(1)
			return fmt.Errorf("validate: %w", Permanent(sentinel))
		})
		if !errors.Is(err, sentinel) || err.Error() != "validate: bad input" {
			t.Fatalf("got %v, want wrapped sentinel with unchanged message", err)
		}
		if n := attempts.Load(); n != 1 {
			t.Fatalf("permanent error attempted %d times, want 1", n)
		}
		if Permanent(nil) != nil {
			t.Fatal("Permanent(nil) != nil")
		}
	})
}

func TestRetry_PanicNotRetriedAndHasStack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1), WithMaxRetries(3), WithRetryDelay(time.Millisecond))
		defer p.Close()
		cause := errors.New("nil map")
		var attempts atomic.Int32
		err := p.SubmitWait(context.Background(), func(context.Context) error {
			attempts.Add(1)
			panic(cause)
		})
		if n := attempts.Load(); n != 1 {
			t.Fatalf("panicking job attempted %d times, want 1", n)
		}
		perr, ok := errors.AsType[*PanicError](err)
		if !ok || !errors.Is(err, ErrJobPanic) || !errors.Is(err, cause) {
			t.Fatalf("got %v, want *PanicError matching ErrJobPanic and the cause", err)
		}
		if len(perr.Stack) == 0 || perr.Value != cause {
			t.Fatalf("PanicError missing stack or value: %+v", perr)
		}
	})
}

func TestRetry_ForwardedPanicErrorIsRetried(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1), WithMaxRetries(2), WithRetryDelay(time.Millisecond))
		defer p.Close()
		var attempts atomic.Int32
		err := p.SubmitWait(context.Background(), func(context.Context) error {
			if attempts.Add(1) < 3 {
				// e.g. the result of SubmitWait on another pool whose job panicked
				return &PanicError{Value: "inner job"}
			}
			return nil
		})
		if err != nil || attempts.Load() != 3 {
			t.Fatalf("got err=%v attempts=%d, want nil and 3: only this job's own panics skip retries", err, attempts.Load())
		}
	})
}

func TestRetry_CancellationDoesNotMaskPanicOrPermanent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var got atomic.Pointer[error] // not atomic.Value: the errors differ in concrete type
		p, _ := NewPool(WithWorkers(1), WithMaxRetries(3),
			WithHooks(Hooks{OnJobFailed: func(_ Job, err error, _ time.Duration) { got.Store(&err) }}))
		defer p.Close()
		sentinel := errors.New("bad input")
		for name, fail := range map[string]func(){
			"panic":     func() { panic("boom") },
			"permanent": func() {},
		} {
			ctx, cancel := context.WithCancel(context.Background())
			_ = p.SubmitContext(ctx, JobFunc(func(context.Context) error {
				cancel() // the job's context is cancelled as it fails
				fail()
				return Permanent(sentinel)
			}))
			synctest.Wait()
			err := *got.Load()
			if want := map[string]error{"panic": ErrJobPanic, "permanent": sentinel}[name]; !errors.Is(err, want) {
				t.Fatalf("%s: OnJobFailed got %v, want %v", name, err, want)
			}
		}
	})
}

func TestRetry_CancelledDuringDelayNotCountedAsRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(1), WithMaxRetries(1), WithRetryDelay(time.Hour))
		defer p.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = p.SubmitContext(ctx, JobFunc(func(context.Context) error { return errors.New("x") }))
		time.Sleep(2 * time.Second) // cancelled during the 1h retry delay
		synctest.Wait()
		if s := p.Snapshot(); s.JobsRetried != 0 || s.JobsFailed != 1 {
			t.Fatalf("JobsRetried=%d JobsFailed=%d, want 0 and 1", s.JobsRetried, s.JobsFailed)
		}
	})
}

func TestRetry_DelayNoOverflow(t *testing.T) {
	p := &Pool{cfg: Config{RetryDelay: 10 * time.Second, RetryBackoff: true}}
	for attempt := 1; attempt <= 100; attempt++ {
		if d := p.retryDelay(attempt); d <= 0 {
			t.Fatalf("attempt %d: delay %v overflowed", attempt, d)
		}
	}
	if d := p.retryDelay(100); d != math.MaxInt64 {
		t.Fatalf("uncapped delay should saturate, got %v", d)
	}
	p.cfg.MaxRetryDelay = time.Minute
	if d := p.retryDelay(100); d != time.Minute {
		t.Fatalf("capped delay: got %v, want 1m", d)
	}
}

func TestRetry_Jitter(t *testing.T) {
	p := &Pool{cfg: Config{RetryDelay: time.Second, RetryJitter: 0.5}}
	varied := false
	for range 200 {
		d := p.retryDelay(1)
		if d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("delay %v outside [500ms, 1s]", d)
		}
		if d != time.Second {
			varied = true
		}
	}
	if !varied {
		t.Fatal("jitter never changed the delay")
	}
}

func TestRetry_InvalidConfig(t *testing.T) {
	for name, opt := range map[string]Option{
		"negative MaxRetryDelay": WithMaxRetryDelay(-time.Second),
		"jitter below 0":         WithRetryJitter(-0.1),
		"jitter above 1":         WithRetryJitter(1.5),
		"jitter NaN":             WithRetryJitter(math.NaN()),
	} {
		if _, err := NewPool(opt); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("%s: got %v, want ErrInvalidConfig", name, err)
		}
	}
}

func TestHooks_PanicsRecovered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := func() { panic("hook bug") }
		p, _ := NewPool(WithWorkers(1),
			WithPanicHandler(func(Job, any) { boom() }),
			WithHooks(Hooks{
				OnWorkerStart: func(int) { boom() },
				OnWorkerStop:  func(int) { boom() },
				OnJobStart:    func(Job) { boom() },
				OnJobComplete: func(Job, time.Duration) { boom() },
				OnJobFailed:   func(Job, error, time.Duration) { boom() },
			}))

		var ran atomic.Int32
		for _, fail := range []bool{false, true} {
			_ = p.SubmitWait(context.Background(), func(context.Context) error {
				ran.Add(1)
				if fail {
					panic("job bug") // also exercises PanicHandler
				}
				return nil
			})
		}
		_ = p.Close()
		if n := ran.Load(); n != 2 {
			t.Fatalf("ran %d jobs, want 2 despite panicking hooks", n)
		}
	})
}
