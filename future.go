package worker

import (
	"context"
	"sync"
)

// Future represents an asynchronous computation that will produce a value of type T.
//
//	future := worker.SubmitTyped[int](pool, func(ctx context.Context) (int, error) {
//	    return computeExpensiveThing(ctx)
//	})
//	result, err := future.Await(ctx)
type Future[T any] struct {
	done chan struct{} // closed once value and err are final

	mu       sync.Mutex // guards the fields below
	attempt  int        // incremented per attempt; only the latest may set value
	resolved bool
	value    T
	err      error
}

// SubmitTyped submits a typed function and returns a [Future] for the result.
// With pool retries enabled, the Future resolves with the outcome of the final
// attempt. If fn panics, the Future resolves with an error wrapping [ErrJobPanic].
func SubmitTyped[T any](pool *Pool, fn func(ctx context.Context) (T, error)) *Future[T] {
	f := &Future[T]{done: make(chan struct{})}

	job := JobFunc(func(ctx context.Context) error {
		// Middleware may run fn on another goroutine and return early, so an
		// abandoned attempt can finish after a newer one or after resolve.
		// Tagging attempts keeps such late results from leaking into the Future.
		n := f.beginAttempt()
		val, err := fn(ctx)
		f.setValue(n, val)
		return err
	})

	err := pool.submit(context.Background(), context.Background(), job, f.resolve, true)
	if err != nil {
		f.resolve(err)
	}

	return f
}

// beginAttempt starts a new attempt, resetting value so a panicking attempt
// resolves with the zero value, and returns the attempt's number.
func (f *Future[T]) beginAttempt() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempt++
	var zero T
	if !f.resolved {
		f.value = zero
	}
	return f.attempt
}

func (f *Future[T]) setValue(attempt int, v T) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.resolved && attempt == f.attempt {
		f.value = v
	}
}

func (f *Future[T]) resolve(err error) {
	f.mu.Lock()
	f.resolved = true
	f.err = err
	f.mu.Unlock()
	close(f.done)
}

// Await blocks until the result is available or the context expires.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	select {
	case <-f.done:
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.value, f.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Done reports whether the result is already available (non-blocking).
func (f *Future[T]) Done() bool {
	select {
	case <-f.done:
		return true
	default:
		return false
	}
}
