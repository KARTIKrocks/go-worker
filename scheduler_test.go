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

// inDSTGap is a clock time that does not exist on spring-forward days in the
// zones tested here (clocks jump 02:00 -> 03:00).
const inDSTGap = "02:30"

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

func TestDailySchedule_TimeInDSTGap(t *testing.T) {
	ny, _ := time.LoadLocation("America/New_York")
	havana, err := time.LoadLocation("America/Havana") // clocks jump at midnight
	if err != nil {
		t.Fatal(err)
	}
	// time.Date resolves gap times backwards in zones west of UTC and
	// forwards in zones east of it; both must give the same result.
	berlin, _ := time.LoadLocation("Europe/Berlin")
	sydney, _ := time.LoadLocation("Australia/Sydney")
	edt := time.FixedZone("EDT", -4*3600)
	cdt := time.FixedZone("CDT", -4*3600)
	cest := time.FixedZone("CEST", 2*3600)
	aedt := time.FixedZone("AEDT", 11*3600)
	for name, tc := range map[string]struct {
		clocks      []string
		after, want time.Time
	}{
		// 2026-03-08 02:00 -> 03:00 in New York: 02:30 does not exist.
		"gap": {[]string{inDSTGap}, time.Date(2026, 3, 8, 0, 0, 0, 0, ny), time.Date(2026, 3, 8, 3, 30, 0, 0, edt)},
		// 2026-03-08 00:00 -> 01:00 in Havana: 00:30 does not exist.
		"gap at midnight": {[]string{"00:30"}, time.Date(2026, 3, 7, 12, 0, 0, 0, havana), time.Date(2026, 3, 8, 1, 30, 0, 0, cdt)},
		// 2026-03-29 02:00 -> 03:00 in Berlin (east of UTC).
		"gap east of UTC": {[]string{inDSTGap}, time.Date(2026, 3, 29, 0, 0, 0, 0, berlin), time.Date(2026, 3, 29, 3, 30, 0, 0, cest)},
		// 2026-10-04 02:00 -> 03:00 in Sydney.
		"gap southern hemisphere": {[]string{inDSTGap}, time.Date(2026, 10, 4, 0, 0, 0, 0, sydney), time.Date(2026, 10, 4, 3, 30, 0, 0, aedt)},
		// 02:30 shifts to 03:30, which is after 03:15: the earliest wins.
		"shifted time after a later clock time": {[]string{inDSTGap, "03:15"}, time.Date(2026, 3, 8, 0, 0, 0, 0, ny), time.Date(2026, 3, 8, 3, 15, 0, 0, edt)},
		"same, east of UTC":                     {[]string{inDSTGap, "03:15"}, time.Date(2026, 3, 29, 0, 0, 0, 0, berlin), time.Date(2026, 3, 29, 3, 15, 0, 0, cest)},
	} {
		s, _ := NewDailySchedule(tc.clocks...)
		if got := s.Next(tc.after); !got.Equal(tc.want) {
			t.Errorf("%s: got %v, want %v", name, got, tc.want)
		}
	}
}

func TestCron_DSTGapDoesNotHang(t *testing.T) {
	ny, _ := time.LoadLocation("America/New_York")
	from := time.Date(2026, 3, 8, 0, 30, 0, 0, ny) // clocks jump 02:00 -> 03:00
	for expr, want := range map[string]time.Time{
		// 02:xx does not exist that day, so it is skipped.
		"0 2 * * *":  time.Date(2026, 3, 9, 2, 0, 0, 0, ny),
		"30 2 * * *": time.Date(2026, 3, 9, 2, 30, 0, 0, ny),
		"0 3 * * *":  time.Date(2026, 3, 8, 3, 0, 0, 0, ny),
	} {
		c, err := ParseCron(expr)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan time.Time, 1)
		go func() { done <- c.Next(from) }()
		select {
		case got := <-done:
			if !got.Equal(want) {
				t.Errorf("%q: got %v, want %v", expr, got, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("%q: Next hung in the DST gap", expr)
		}
	}
}

func TestOnceSchedule_NextIsStrictlyAfter(t *testing.T) {
	at := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := &OnceSchedule{At: at}
	if got := s.Next(at.Add(-time.Nanosecond)); !got.Equal(at) {
		t.Fatalf("before At: got %v, want %v", got, at)
	}
	if got := s.Next(at); !got.IsZero() {
		t.Fatalf("Next(At) = %v, want zero: it must be strictly after its argument", got)
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

// The exact interleaving CI hit: a Trigger fails to claim the slot, the
// running run then finishes and releases the slot seeing an empty queue,
// and only then does the Trigger queue its run.
func TestScheduler_QueueAfterSlotReleasedStillRuns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newTestPool(t)
		s := NewScheduler(p)
		defer s.Stop()
		var runs atomic.Int32
		s.EveryFunc("t", time.Hour, func(context.Context) error { runs.Add(1); return nil },
			WithOverlapPolicy(OverlapQueue))
		task := s.tasks["t"]

		task.running.Store(1)  // run A holds the slot; the Trigger's claim fails
		s.finishRun(task)      // A finishes, sees no queued run, releases
		if !s.queueRun(task) { // the Trigger now queues
			t.Fatal("queueRun rejected")
		}
		synctest.Wait()
		if n := runs.Load(); n != 1 {
			t.Fatalf("queued run ran %d times, want 1", n)
		}
	})
}

// Every Trigger that reports true must eventually run. This stress test is
// what caught the queueRun race in CI; the test above pins it down exactly.
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
