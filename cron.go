package worker

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

// CronSchedule fires based on cron-like expressions.
//
// Format: "minute hour day-of-month month day-of-week"
//
// Each field accepts *, single values, ranges (1-5), lists (1,3,5) and steps
// (*/15, 0-30/10, 5/20). Months accept JAN-DEC and weekdays SUN-SAT (case
// insensitive); weekday 7 is also Sunday. The macros @yearly (@annually),
// @monthly, @weekly, @daily (@midnight) and @hourly are supported.
//
// Times that do not exist because of a daylight-saving jump are skipped that
// day.
//
// As in standard cron, when both day-of-month and day-of-week are restricted
// a day matches if either does; a field starting with "*" (such as "*/2")
// counts as unrestricted for this rule.
type CronSchedule struct {
	minutes     []int
	hours       []int
	days        []int
	months      []int
	weekdays    []int
	expression  string
	allDays     bool // day-of-month field starts with "*" (selects AND, see dayMatches)
	allWeekdays bool // day-of-week field starts with "*" (selects AND, see dayMatches)
}

var cronMacros = map[string]string{
	"@yearly":   "0 0 1 1 *",
	"@annually": "0 0 1 1 *",
	"@monthly":  "0 0 1 * *",
	"@weekly":   "0 0 * * 0",
	"@daily":    "0 0 * * *",
	"@midnight": "0 0 * * *",
	"@hourly":   "0 * * * *",
}

var (
	cronMonthNames = map[string]int{
		"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
		"JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
	}
	cronWeekdayNames = map[string]int{
		"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6,
	}
)

// ParseCron parses a standard 5-field cron expression or a macro.
//
// Examples:
//
//	"*/15 * * * *"    — every 15 minutes
//	"0 9 * * MON-FRI" — 9 AM on weekdays
//	"0 0 1 * *"       — midnight on the 1st of each month
//	"@hourly"         — at the start of every hour
func ParseCron(expr string) (*CronSchedule, error) {
	fields := expr
	if macro, ok := cronMacros[strings.ToLower(strings.TrimSpace(expr))]; ok {
		fields = macro
	}
	parts := strings.Fields(fields)
	if len(parts) != 5 {
		return nil, fmt.Errorf("invalid cron expression: expected 5 fields, got %d", len(parts))
	}

	minutes, err := parseCronField(parts[0], 0, 59, nil)
	if err != nil {
		return nil, fmt.Errorf("minute field: %w", err)
	}
	hours, err := parseCronField(parts[1], 0, 23, nil)
	if err != nil {
		return nil, fmt.Errorf("hour field: %w", err)
	}
	days, err := parseCronField(parts[2], 1, 31, nil)
	if err != nil {
		return nil, fmt.Errorf("day field: %w", err)
	}
	months, err := parseCronField(parts[3], 1, 12, cronMonthNames)
	if err != nil {
		return nil, fmt.Errorf("month field: %w", err)
	}
	// Accept 7 as Sunday, then fold it into 0.
	weekdays, err := parseCronField(parts[4], 0, 7, cronWeekdayNames)
	if err != nil {
		return nil, fmt.Errorf("weekday field: %w", err)
	}
	if i := slices.Index(weekdays, 7); i >= 0 {
		weekdays = slices.Delete(weekdays, i, i+1)
		if !slices.Contains(weekdays, 0) {
			weekdays = slices.Insert(weekdays, 0, 0)
		}
	}

	return &CronSchedule{
		minutes:     minutes,
		hours:       hours,
		days:        days,
		months:      months,
		weekdays:    weekdays,
		expression:  expr,
		allDays:     strings.HasPrefix(parts[2], "*"),
		allWeekdays: strings.HasPrefix(parts[4], "*"),
	}, nil
}

// Next implements [Schedule].
func (s *CronSchedule) Next(after time.Time) time.Time {
	// Truncate to the next whole minute in the same timezone.
	t := after.Add(time.Minute)
	t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), 0, 0, t.Location())
	limit := after.Add(5 * 365 * 24 * time.Hour)

	for t.Before(limit) {
		var next time.Time
		switch {
		case !slices.Contains(s.months, int(t.Month())):
			next = time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, t.Location())
		case !s.dayMatches(t):
			next = time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, t.Location())
		case !slices.Contains(s.hours, t.Hour()):
			next = time.Date(t.Year(), t.Month(), t.Day(), t.Hour()+1, 0, 0, 0, t.Location())
		case !slices.Contains(s.minutes, t.Minute()):
			next = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute()+1, 0, 0, t.Location())
		default:
			return t
		}
		// time.Date moves a local time inside a spring-forward gap backwards
		// (02:00 becomes 01:00 EST), which would loop forever. Always make
		// progress; times inside the gap are skipped that day.
		if !next.After(t) {
			next = t.Add(time.Minute)
		}
		t = next
	}
	return time.Time{}
}

// dayMatches applies the day-of-month / day-of-week rule: if both fields are
// restricted (neither starts with "*"), either may match; otherwise both must.
// A field such as "*/2" still only matches its own values; the "*" prefix
// only selects AND over OR.
func (s *CronSchedule) dayMatches(t time.Time) bool {
	dayMatch := slices.Contains(s.days, t.Day())
	wdayMatch := slices.Contains(s.weekdays, int(t.Weekday()))
	if !s.allDays && !s.allWeekdays {
		return dayMatch || wdayMatch
	}
	return dayMatch && wdayMatch
}

// String returns the original cron expression.
func (s *CronSchedule) String() string { return s.expression }

// parseCronField parses one comma-separated cron field into the sorted,
// de-duplicated values it matches within [lo, hi]. names maps case-insensitive
// names (JAN, MON) to values; it may be nil.
func parseCronField(field string, lo, hi int, names map[string]int) ([]int, error) {
	matched := make([]bool, hi+1)
	for part := range strings.SplitSeq(field, ",") {
		start, end, step, err := parseCronPart(part, lo, hi, names)
		if err != nil {
			return nil, err
		}
		for i := start; i <= end; i += step {
			matched[i] = true
		}
	}

	var result []int
	for i, ok := range matched {
		if ok {
			result = append(result, i)
		}
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("empty field %q", field)
	}
	return result, nil
}

// parseCronPart parses one list element of a cron field ("*", "5", "1-5",
// "*/15", "0-30/10" or "5/20") into an inclusive range within [lo, hi] and a step.
func parseCronPart(part string, lo, hi int, names map[string]int) (start, end, step int, err error) {
	rng, stepStr, hasStep := strings.Cut(part, "/")
	step = 1
	if hasStep {
		if step, err = strconv.Atoi(stepStr); err != nil || step <= 0 {
			return 0, 0, 0, fmt.Errorf("invalid step in %q", part)
		}
	}

	switch {
	case rng == "*":
		start, end = lo, hi
	case strings.Contains(rng, "-"):
		a, b, _ := strings.Cut(rng, "-")
		if start, err = parseCronValue(a, names); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid range start in %q", part)
		}
		if end, err = parseCronValue(b, names); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid range end in %q", part)
		}
	default:
		if start, err = parseCronValue(rng, names); err != nil {
			return 0, 0, 0, fmt.Errorf("invalid value %q", part)
		}
		end = start
		if hasStep {
			end = hi // "5/20" means from 5 to the maximum, every 20
		}
	}
	if start < lo || end > hi || start > end {
		return 0, 0, 0, fmt.Errorf("%q out of bounds [%d, %d]", part, lo, hi)
	}
	return start, end, step, nil
}

// parseCronValue parses a number or, if names is non-nil, a name such as JAN.
func parseCronValue(s string, names map[string]int) (int, error) {
	if v, ok := names[strings.ToUpper(s)]; ok {
		return v, nil
	}
	return strconv.Atoi(s)
}
