package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// Permanent wraps err so the pool does not retry it. The returned error
// unwraps to err, so errors.Is and errors.As see through it. Permanent(nil)
// returns nil.
//
//	if errors.Is(err, sql.ErrNoRows) {
//	    return worker.Permanent(err) // retrying will not help
//	}
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// isPermanent reports whether err was wrapped with [Permanent].
func isPermanent(err error) bool {
	_, ok := errors.AsType[*permanentError](err)
	return ok
}

// PanicError is the error a job returns when it panics. It matches
// [ErrJobPanic] with errors.Is, and unwraps to the panic value if that value
// is itself an error.
type PanicError struct {
	Value any    // the value passed to panic
	Stack []byte // stack trace of the panicking goroutine
}

func (e *PanicError) Error() string { return fmt.Sprintf("%v: %v", ErrJobPanic, e.Value) }

// Is reports whether target is [ErrJobPanic].
func (e *PanicError) Is(target error) bool { return target == ErrJobPanic }

// Unwrap returns the panic value if it is an error, or nil.
func (e *PanicError) Unwrap() error {
	err, _ := e.Value.(error)
	return err
}

// retryDelay returns the delay before the retry following the given attempt,
// applying exponential backoff, the MaxRetryDelay cap and jitter when configured.
func (p *Pool) retryDelay(attempt int) time.Duration {
	delay := p.cfg.RetryDelay
	if p.cfg.RetryBackoff {
		// delay << shift, saturating instead of overflowing into a negative delay.
		shift := min(attempt-1, 62)
		if delay > time.Duration(math.MaxInt64>>shift) {
			delay = math.MaxInt64
		} else {
			delay <<= shift
		}
	}
	if p.cfg.MaxRetryDelay > 0 && delay > p.cfg.MaxRetryDelay {
		delay = p.cfg.MaxRetryDelay
	}
	if p.cfg.RetryJitter > 0 {
		// Subtract up to RetryJitter of the delay, so the cap is never exceeded.
		delay -= time.Duration(rand.Float64() * p.cfg.RetryJitter * float64(delay)) //nolint:gosec // retry jitter is not security-sensitive
	}
	return delay
}

// sleepCtx waits for d or until ctx is done, returning ctx.Err() in the latter case.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
