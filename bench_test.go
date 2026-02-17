package worker_test

import (
	"context"
	"testing"

	"github.com/KARTIKrocks/go-worker"
)

func BenchmarkSubmit(b *testing.B) {
	pool, _ := worker.NewPool(
		worker.WithWorkers(8),
		worker.WithQueueSize(1024),
		worker.WithJobTimeout(0),
	)
	defer pool.Close()

	noop := func(ctx context.Context) error { return nil }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.Submit(noop)
		}
	})
}

func BenchmarkSubmitWait(b *testing.B) {
	pool, _ := worker.NewPool(
		worker.WithWorkers(8),
		worker.WithQueueSize(1024),
		worker.WithJobTimeout(0),
	)
	defer pool.Close()

	ctx := context.Background()
	noop := func(ctx context.Context) error { return nil }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.SubmitWait(ctx, noop)
		}
	})
}

func BenchmarkTrySubmit(b *testing.B) {
	pool, _ := worker.NewPool(
		worker.WithWorkers(8),
		worker.WithQueueSize(4096),
		worker.WithJobTimeout(0),
	)
	defer pool.Close()

	noop := func(ctx context.Context) error { return nil }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.TrySubmit(noop)
		}
	})
}

func BenchmarkSubmitWithMiddleware(b *testing.B) {
	noop := func(next func(ctx context.Context) error) func(ctx context.Context) error {
		return func(ctx context.Context) error { return next(ctx) }
	}

	pool, _ := worker.NewPool(
		worker.WithWorkers(8),
		worker.WithQueueSize(1024),
		worker.WithJobTimeout(0),
		worker.WithMiddleware(noop, noop, noop),
	)
	defer pool.Close()

	ctx := context.Background()
	fn := func(ctx context.Context) error { return nil }

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.SubmitWait(ctx, fn)
		}
	})
}
