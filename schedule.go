package worker

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Schedule determines when a task should next run.
type Schedule interface {
	// Next returns the next execution time after the given time.
	// A zero time means the schedule has no more executions.
	Next(after time.Time) time.Time
}

// IntervalSchedule fires at fixed intervals. A non-positive Interval never fires.
type IntervalSchedule struct {
	Interval time.Duration
}

// Next implements [Schedule].
func (s *IntervalSchedule) Next(after time.Time) time.Time {
	if s.Interval <= 0 {
		return time.Time{}
	}
	return after.Add(s.Interval)
}

// FixedTimeSchedule fires at a start time, then repeats at a fixed interval.
// A non-positive Interval fires once at Start.
type FixedTimeSchedule struct {
	Start    time.Time
	Interval time.Duration
}

// Next implements [Schedule].
func (s *FixedTimeSchedule) Next(after time.Time) time.Time {
	if after.Before(s.Start) {
		return s.Start
	}
	if s.Interval <= 0 {
		return time.Time{}
	}
	elapsed := after.Sub(s.Start)
	periods := int64(elapsed / s.Interval)
	return s.Start.Add(time.Duration(periods+1) * s.Interval)
}

// OnceSchedule fires exactly once at a given time.
type OnceSchedule struct {
	At   time.Time
	done atomic.Bool
}

// Next implements [Schedule].
func (s *OnceSchedule) Next(after time.Time) time.Time {
	if s.done.Load() {
		return time.Time{}
	}
	if !after.Before(s.At) { // Next must return a time strictly after `after`
		s.done.Store(true)
		return time.Time{}
	}
	return s.At
}

// DailySchedule fires at specific HH:MM wall-clock times each day, in the
// time zone of the time passed to Next. A time skipped by a daylight-saving
// change (02:30 when clocks jump from 02:00 to 03:00) fires after the jump
// (03:30).
type DailySchedule struct {
	times []time.Duration // time of day as hours+minutes, sorted ascending
}

// NewDailySchedule creates a schedule from "HH:MM" strings.
func NewDailySchedule(times ...string) (*DailySchedule, error) {
	durations := make([]time.Duration, 0, len(times))
	for _, t := range times {
		parts := strings.Split(t, ":")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid time format %q (expected HH:MM)", t)
		}
		h, err := strconv.Atoi(parts[0])
		if err != nil || h < 0 || h > 23 {
			return nil, fmt.Errorf("invalid hour in %q", t)
		}
		m, err := strconv.Atoi(parts[1])
		if err != nil || m < 0 || m > 59 {
			return nil, fmt.Errorf("invalid minute in %q", t)
		}
		durations = append(durations, time.Duration(h)*time.Hour+time.Duration(m)*time.Minute)
	}
	slices.Sort(durations)
	return &DailySchedule{times: durations}, nil
}

// Next implements [Schedule].
func (s *DailySchedule) Next(after time.Time) time.Time {
	for day := range 3 {
		// After DST adjustment, clock order is not instant order (02:30 can
		// become 03:30, after 03:15), so take the earliest candidate each day.
		var best time.Time
		for _, t := range s.times {
			c := dailyAt(after.Year(), after.Month(), after.Day()+day, t, after.Location())
			if c.After(after) && (best.IsZero() || c.Before(best)) {
				best = c
			}
		}
		if !best.IsZero() {
			return best
		}
	}
	return time.Time{}
}

// dailyAt returns time of day tod on the given date. It builds the time from
// the date and clock rather than adding a duration to midnight, because a
// daylight-saving day is 23 or 25 hours long. A time inside a spring-forward
// gap does not exist; it is shifted forward by the gap (02:30 becomes 03:30
// when clocks jump 02:00 to 03:00). time.Date resolves such a time forwards
// in some zones and backwards in others, so check which way it went.
func dailyAt(y int, mo time.Month, d int, tod time.Duration, loc *time.Location) time.Time {
	h, m := int(tod/time.Hour), int(tod%time.Hour/time.Minute)
	c := time.Date(y, mo, d, h, m, 0, 0, loc)
	want, have := h*60+m, c.Hour()*60+c.Minute()
	intended := time.Date(y, mo, d, 0, 0, 0, 0, time.UTC)
	actual := time.Date(c.Year(), c.Month(), c.Day(), 0, 0, 0, 0, time.UTC)
	switch {
	case actual.After(intended) || (actual.Equal(intended) && have >= want):
		return c // exact, or already moved forward past the gap
	case actual.Equal(intended):
		return c.Add(time.Duration(want-have) * time.Minute) // moved back: shift forward
	default:
		return c.Add(time.Duration(want-have+24*60) * time.Minute) // moved back to the previous day
	}
}
