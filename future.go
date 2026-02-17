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
	ch   chan result[T]
	val  result[T]
	once sync.Once
}

type result[T any] struct {
	value T
	err   error
}

// SubmitTyped submits a typed function and returns a [Future] for the result.
func SubmitTyped[T any](pool *Pool, fn func(ctx context.Context) (T, error)) *Future[T] {
	f := &Future[T]{
		ch: make(chan result[T], 1),
	}

	job := JobFunc(func(ctx context.Context) error {
		val, err := fn(ctx)
		f.ch <- result[T]{value: val, err: err}
		return err
	})

	if err := pool.SubmitJob(job); err != nil {
		var zero T
		f.ch <- result[T]{value: zero, err: err}
	}

	return f
}

// Await blocks until the result is available or the context expires.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	// Fast path: result already cached.
	if f.done() {
		return f.val.value, f.val.err
	}

	select {
	case r := <-f.ch:
		f.once.Do(func() { f.val = r })
		// Put it back so other callers / Done() can also read it.
		select {
		case f.ch <- r:
		default:
		}
		return f.val.value, f.val.err
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

// Done reports whether the result is already available (non-blocking).
func (f *Future[T]) Done() bool {
	return f.done()
}

// done checks if the result has been consumed and cached.
func (f *Future[T]) done() bool {
	select {
	case r := <-f.ch:
		f.once.Do(func() { f.val = r })
		// Put it back so subsequent calls can also read it.
		select {
		case f.ch <- r:
		default:
		}
		return true
	default:
		return false
	}
}
