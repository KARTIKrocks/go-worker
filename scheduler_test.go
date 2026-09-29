package worker

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
	_ "time/tzdata" // so the DST tests do not depend on the host's zone database
)

// ---------- DailySchedule ----------

func TestDailySchedule_DST(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	s, _ := NewDailySchedule("09:00")
	for name, tc := range map[string]struct{ after, want time.Time }{
		// 2026-03-08: clocks go forward (23-hour day).
		"spring forward, same day": {time.Date(2026, 3, 8, 0, 30, 0, 0, ny), time.Date(2026, 3, 8, 9, 0, 0, 0, ny)},
		"spring forward, next day": {time.Date(2026, 3, 7, 10, 0, 0, 0, ny), time.Date(2026, 3, 8, 9, 0, 0, 0, ny)},
		// 2026-11-01: clocks go back (25-hour day).
		"fall back, same day": {time.Date(2026, 11, 1, 0, 30, 0, 0, ny), time.Date(2026, 11, 1, 9, 0, 0, 0, ny)},
		"fall back, next day": {time.Date(2026, 10, 31, 10, 0, 0, 0, ny), time.Date(2026, 11, 1, 9, 0, 0, 0, ny)},
	} {
		if got := s.Next(tc.after); !got.Equal(tc.want) {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

// ---------- Cron ----------

func TestParseCron_ExtendedSyntax(t *testing.T) {
	for _, tc := range []struct {
		expr  string
		field func(*CronSchedule) []int
		want  []int
	}{
		{"1-10/3 * * * *", func(c *CronSchedule) []int { return c.minutes }, []int{1, 4, 7, 10}},
		{"5/20 * * * *", func(c *CronSchedule) []int { return c.minutes }, []int{5, 25, 45}},
		{"0,30,0 * * * *", func(c *CronSchedule) []int { return c.minutes }, []int{0, 30}},
		{"0 0 * * 7", func(c *CronSchedule) []int { return c.weekdays }, []int{0}},
		{"0 0 * * 5-7", func(c *CronSchedule) []int { return c.weekdays }, []int{0, 5, 6}},
		{"0 0 * * mon-FRI", func(c *CronSchedule) []int { return c.weekdays }, []int{1, 2, 3, 4, 5}},
		{"0 0 1 jan,DEC *", func(c *CronSchedule) []int { return c.months }, []int{1, 12}},
		{"@daily", func(c *CronSchedule) []int { return c.hours }, []int{0}},
		{"@HOURLY", func(c *CronSchedule) []int { return c.minutes }, []int{0}},
		{"@weekly", func(c *CronSchedule) []int { return c.weekdays }, []int{0}},
	} {
		c, err := ParseCron(tc.expr)
		if err != nil {
			t.Errorf("%q: %v", tc.expr, err)
			continue
		}
		if got := tc.field(c); !slices.Equal(got, tc.want) {
			t.Errorf("%q: got %v, want %v", tc.expr, got, tc.want)
		}
	}
}

func TestParseCron_ExtendedInvalid(t *testing.T) {
	for _, expr := range []string{
		"1-10/0 * * * *", // zero step
		"* * * * 8",      // weekday out of range
		"* * * * FOO",    // unknown name
		"* * * JAN-FOO *",
		"10-5 * * * *", // reversed range
		"-1 * * * *",   // negative
		"@every 5m",    // unsupported macro
		"* * * JAN *x",
	} {
		if _, err := ParseCron(expr); err == nil {
			t.Errorf("%q: expected error", expr)
		}
	}
}

func TestCron_DayMatching(t *testing.T) {
	date := func(m time.Month, d int) time.Time { return time.Date(2026, m, d, 0, 0, 0, 0, time.UTC) }
	tue := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC) // a Tuesday
	for _, tc := range []struct {
		expr       string
		from, want time.Time
	}{
		// A "*/n" day field still matches only its own values.
		{"0 0 */2 * *", date(1, 1), date(1, 3)},
		{"0 0 * * */2", tue, date(10, 1)}, // weekdays 0,2,4,6: Thursday
		// "*/2" selects AND with the other field: an odd day that is a Monday.
		{"0 0 */2 * 1", tue, date(10, 5)},
		{"0 0 */2 * 1", date(10, 6), date(10, 19)}, // skips Monday the 12th
		// Both restricted: either the 1st or a Monday, whichever is first.
		{"0 0 1 * 1", tue, date(10, 1)},
		{"0 0 * * 1", tue, date(10, 5)},
	} {
		c, err := ParseCron(tc.expr)
		if err != nil {
			t.Fatal(err)
		}
		if got := c.Next(tc.from); !got.Equal(tc.want) {
			t.Errorf("%q from %v: got %v, want %v", tc.expr, tc.from, got, tc.want)
		}
	}
}

// ---------- Scheduler lifecycle ----------

func TestScheduler_StopIsPermanent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t)
		var runs atomic.Int32
		s := NewScheduler(p, WithTickInterval(10*time.Millisecond))
		s.EveryFunc("t", 10*time.Millisecond, func(context.Context) error { runs.Add(1); return nil })
		s.Start()
		time.Sleep(55 * time.Millisecond)
		s.Stop()
		synctest.Wait()
		before := runs.Load()

		s.Start()
		time.Sleep(100 * time.Millisecond)
		synctest.Wait()
		if s.IsRunning() {
			t.Fatal("IsRunning() = true after Start on a stopped scheduler")
		}
		if n := runs.Load(); n != before {
			t.Fatalf("ran %d more times after Stop", n-before)
		}

		// Stop before Start is also permanent.
		s2 := NewScheduler(p)
		s2.Stop()
		s2.Start()
		if s2.IsRunning() {
			t.Fatal("scheduler started after Stop")
		}
	})
}

// ---------- Trigger ----------

func TestScheduler_TriggerRespectsTaskSettings(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(4))
		defer p.Close()
		s := NewScheduler(p)
		defer s.Stop()

		release := make(chan struct{})
		var runs atomic.Int32
		job := func(context.Context) error { runs.Add(1); <-release; return nil }

		s.EveryFunc("skip", time.Hour, job)
		if !s.Trigger("skip") {
			t.Fatal("first Trigger rejected")
		}
		synctest.Wait()
		if s.Trigger("skip") {
			t.Fatal("Trigger started a second run while running under OverlapSkip")
		}

		s.EveryFunc("queue", time.Hour, job, WithOverlapPolicy(OverlapQueue))
		if !s.Trigger("queue") {
			t.Fatal("first Trigger under OverlapQueue rejected")
		}
		if !s.Trigger("queue") {
			t.Fatal("second Trigger under OverlapQueue should queue a run")
		}
		synctest.Wait()
		if n := runs.Load(); n != 2 {
			t.Fatalf("runs = %d before release, want 2 (one per task)", n)
		}
		close(release)
		synctest.Wait()
		if n := runs.Load(); n != 3 {
			t.Fatalf("runs = %d after release, want 3 (queued run ran)", n)
		}

		s.EveryFunc("paused", time.Hour, func(context.Context) error { return nil })
		s.Pause("paused")
		if s.Trigger("paused") {
			t.Fatal("Trigger ran a paused task")
		}

		s.EveryFunc("limited", time.Hour, func(context.Context) error { return nil }, WithMaxRuns(1))
		if !s.Trigger("limited") {
			t.Fatal("Trigger rejected the first run")
		}
		synctest.Wait()
		if s.Trigger("limited") {
			t.Fatal("Trigger exceeded WithMaxRuns")
		}

		if s.Trigger("missing") {
			t.Fatal("Trigger on a missing task returned true")
		}
	})
}

func TestScheduler_MaxRunsUnderConcurrentTriggers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(4))
		defer p.Close()
		s := NewScheduler(p)
		defer s.Stop()
		var runs atomic.Int32
		s.EveryFunc("t", time.Hour, func(context.Context) error { runs.Add(1); return nil },
			WithOverlapPolicy(OverlapAllow), WithMaxRuns(3))

		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() { s.Trigger("t") })
		}
		wg.Wait()
		synctest.Wait()
		if n := runs.Load(); n != 3 {
			t.Fatalf("ran %d times, want 3", n)
		}
	})
}

func TestScheduler_QueueRespectsMaxRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p, _ := NewPool(WithWorkers(2))
		defer p.Close()
		s := NewScheduler(p)
		defer s.Stop()
		release := make(chan struct{})
		s.EveryFunc("t", time.Hour, func(context.Context) error { <-release; return nil },
			WithOverlapPolicy(OverlapQueue), WithMaxRuns(1))
		if !s.Trigger("t") {
			t.Fatal("first Trigger rejected")
		}
		synctest.Wait()
		if s.Trigger("t") {
			t.Fatal("Trigger queued a run beyond WithMaxRuns")
		}
		close(release)
	})
}

// Every Trigger that reports true must eventually run. (The narrow window
// between a run's queue check and its slot release, handled in finishRun, is
// too small to hit reliably here; this checks the overall invariant.)
func TestScheduler_AcceptedTriggersAllRun(t *testing.T) {
	for range 50 {
		synctest.Test(t, func(t *testing.T) {
			p, _ := NewPool(WithWorkers(4))
			defer p.Close()
			s := NewScheduler(p)
			defer s.Stop()
			var runs, accepted atomic.Int32
			s.EveryFunc("t", time.Hour, func(context.Context) error { runs.Add(1); return nil },
				WithOverlapPolicy(OverlapQueue))
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(func() {
					for range 10 {
						if s.Trigger("t") {
							accepted.Add(1)
						}
					}
				})
			}
			wg.Wait()
			synctest.Wait()
			if r, a := runs.Load(), accepted.Load(); r != a {
				t.Fatalf("%d triggers accepted but %d runs happened", a, r)
			}
		})
	}
}

// ---------- Once ----------

func TestScheduler_OnceWithRunImmediateRunsOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t)
		var runs atomic.Int32
		s := NewScheduler(p, WithTickInterval(time.Second))
		s.OnceFunc("t", time.Now().Add(time.Hour), func(context.Context) error { runs.Add(1); return nil },
			WithRunImmediate())
		s.Start()
		time.Sleep(2 * time.Hour)
		s.Stop()
		synctest.Wait()
		if n := runs.Load(); n != 1 {
			t.Fatalf("once task ran %d times, want 1", n)
		}
	})
}

// ---------- Callbacks ----------

func TestScheduler_CallbackPanicsRecovered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t)
		var runs atomic.Int32
		s := NewScheduler(p, WithTickInterval(10*time.Millisecond),
			WithSchedulerLogger(panickyLogger{}), // its Error panics too
			WithOnTaskStart(func(string) { panic("start hook bug") }),
			WithOnTaskEnd(func(string, error, time.Duration) { panic("end hook bug") }))
		s.EveryFunc("t", 10*time.Millisecond, func(context.Context) error { runs.Add(1); return nil })
		s.Start()
		time.Sleep(55 * time.Millisecond)
		s.Stop()
		synctest.Wait()
		if n := runs.Load(); n < 3 {
			t.Fatalf("task ran %d times, want several despite panicking callbacks", n)
		}
	})
}
