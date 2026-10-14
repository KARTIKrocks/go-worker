---
id: schedules
title: Schedules and Cron
description: Cron expression syntax supported by go-worker, daily and interval schedules, time zones and daylight saving, and writing a custom Schedule.
---

# Schedules and Cron

Every scheduled task is driven by a `Schedule`, which answers one question —
when should this next run?

```go
type Schedule interface {
    // Next returns the next run time after the given time.
    // A zero time means there are no more runs.
    Next(after time.Time) time.Time
}
```

The scheduler's convenience methods build the built-in schedules for you.

## Cron expressions

`Cron` and `CronFunc` take a standard five-field expression:

```text
┌───────────── minute        0–59
│ ┌─────────── hour          0–23
│ │ ┌───────── day of month  1–31
│ │ │ ┌─────── month         1–12 or JAN–DEC
│ │ │ │ ┌───── day of week   0–6 or SUN–SAT (7 is also Sunday)
│ │ │ │ │
* * * * *
```

| Syntax | Example | Meaning |
| --- | --- | --- |
| `*` | `*` | every value |
| value | `30` | exactly 30 |
| range | `1-5` | 1 through 5 |
| list | `1,15,30` | each listed value |
| step | `*/15` | every 15th value |
| range with step | `0-30/10` | 0, 10, 20, 30 |
| start with step | `5/20` | 5, 25, 45 — from 5 to the maximum |
| names | `MON-FRI`, `JAN,JUL` | case-insensitive month and weekday names |

```text
*/15 * * * *     every 15 minutes
0 9 * * MON-FRI  09:00 on weekdays
0 0 1 * *        midnight on the 1st of each month
30 2 * * 0       02:30 every Sunday
```

### Macros

| Macro | Equivalent |
| --- | --- |
| `@yearly`, `@annually` | `0 0 1 1 *` |
| `@monthly` | `0 0 1 * *` |
| `@weekly` | `0 0 * * 0` |
| `@daily`, `@midnight` | `0 0 * * *` |
| `@hourly` | `0 * * * *` |

### Day of month and day of week

As in standard cron, when **both** day fields are restricted, a day matches if
**either** does: `0 0 1 * MON` runs on the 1st of the month *and* on every
Monday. A field starting with `*` — including `*/2` — counts as unrestricted
for this rule.

### Parsing

`ParseCron(expr)` parses an expression into a `*CronSchedule` without
scheduling anything — useful to validate user-supplied expressions. Errors
name the offending field.

Seconds fields, `L`, `W`, `#`, and `?` are not supported.

## Daily times

`Daily` and `DailyFunc` take `"HH:MM"` strings in 24-hour time, and the
schedule fires at each of them every day:

```go
sched.DailyFunc("backup", []string{"02:00", "14:00"}, backup)
```

`NewDailySchedule(times...)` builds the same schedule for use with `Schedule`.

## Intervals

| Schedule | Fires |
| --- | --- |
| `IntervalSchedule{Interval: d}` | every `d`, measured from when the previous run was dispatched — used by `Every` |
| `FixedTimeSchedule{Start: t, Interval: d}` | at `t`, then every `d` after it — used by `At` |
| `OnceSchedule{At: t}` | once at `t` — used by `Once` |

`FixedTimeSchedule` keeps runs aligned to `Start` — every 15 minutes past the
hour stays on the quarter hour. A non-positive interval on `FixedTimeSchedule`
fires once at `Start`; on `IntervalSchedule` it never fires.

## Time zones and daylight saving

Cron and daily schedules use the **time zone of the process** — Go's local
time, which comes from the `TZ` environment variable or the system setting.
Containers often default to UTC, so `0 9 * * *` in a container is 09:00 UTC
unless you set `TZ`.

Around daylight-saving changes:

- **Cron** skips a time that does not exist that day — `30 2 * * *` does not run
  on the night clocks jump from 02:00 to 03:00.
- **Daily** runs a skipped time just after the jump instead — `"02:30"` runs at
  03:30 that night.

## Custom schedules

Implement `Schedule` for anything the built-ins do not cover — business days
from a holiday calendar, times fetched from configuration:

```go
type BusinessHours struct{ Every time.Duration }

func (s BusinessHours) Next(after time.Time) time.Time {
    next := after.Add(s.Every)
    for next.Hour() < 9 || next.Hour() >= 17 || isWeekend(next) {
        next = next.Add(s.Every)
    }
    return next
}

sched.ScheduleFunc("poll", BusinessHours{Every: 10 * time.Minute}, poll)
```

`Next` must return a time strictly after `after`, or the zero time to stop the
task. The scheduler checks for due tasks every tick (100ms by default), so
precision is limited to the tick interval.
