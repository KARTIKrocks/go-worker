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
	if after.After(s.At) {
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
	// Build each candidate from the calendar date and clock time rather than
	// adding a duration to midnight: on daylight-saving change days a day is
	// 23 or 25 hours long, and midnight.Add(9h) would not be 09:00.
	for day := range 3 {
		for _, t := range s.times {
			h, m := int(t/time.Hour), int(t%time.Hour/time.Minute)
			c := time.Date(after.Year(), after.Month(), after.Day()+day, h, m, 0, 0, after.Location())
			// A time inside a spring-forward gap does not exist, and time.Date
			// moves it backwards (02:30 becomes 01:30 EST). Shift it forward
			// by the gap instead (02:30 becomes 03:30 EDT), so it still runs
			// that day, after the clock change.
			if diff := (h*60 + m) - (c.Hour()*60 + c.Minute()); diff != 0 {
				if diff < 0 {
					diff += 24 * 60 // normalized back across midnight
				}
				c = c.Add(time.Duration(diff) * time.Minute)
			}
			if c.After(after) {
				return c
			}
		}
	}
	return time.Time{}
}
