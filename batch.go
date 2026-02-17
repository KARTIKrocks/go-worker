package worker

import (
	"context"
)

// Batch processes items in configurable-sized chunks using a worker pool.
// It is useful for bulk database inserts, API calls, or similar fan-out patterns.
//
// Batch is NOT safe for concurrent use. Do not call Add concurrently with
// Process or ProcessAll.
//
//	batch := worker.NewBatch(pool, 100, func(ctx context.Context, users []User) error {
//	    return db.BulkInsert(ctx, users)
//	})
//	batch.Add(users...)
//	err := batch.Process(ctx)
type Batch[T any] struct {
	pool      *Pool
	batchSize int
	items     []T
	processor func(context.Context, []T) error
}

// NewBatch creates a batch processor. The processor function is called once
// per chunk of up to batchSize items.
func NewBatch[T any](pool *Pool, batchSize int, processor func(context.Context, []T) error) *Batch[T] {
	if batchSize < 1 {
		batchSize = 1
	}
	return &Batch[T]{
		pool:      pool,
		batchSize: batchSize,
		processor: processor,
	}
}

// Add appends items to be processed.
func (b *Batch[T]) Add(items ...T) {
	b.items = append(b.items, items...)
}

// Process processes all items in batches using an [ErrorGroup].
// It cancels remaining batches on the first error.
func (b *Batch[T]) Process(ctx context.Context) error {
	if len(b.items) == 0 {
		return nil
	}

	g := NewErrorGroupContext(ctx, b.pool)

	for i := 0; i < len(b.items); i += b.batchSize {
		end := min(i+b.batchSize, len(b.items))
		// Copy the slice to avoid closure capture issues.
		chunk := make([]T, end-i)
		copy(chunk, b.items[i:end])

		g.Go(func(ctx context.Context) error {
			return b.processor(ctx, chunk)
		})
	}

	return g.Wait()
}

// ProcessAll processes all items and collects all errors (does not cancel on first error).
func (b *Batch[T]) ProcessAll(ctx context.Context) []error {
	if len(b.items) == 0 {
		return nil
	}

	g := NewGroupContext(ctx, b.pool)

	for i := 0; i < len(b.items); i += b.batchSize {
		end := min(i+b.batchSize, len(b.items))
		chunk := make([]T, end-i)
		copy(chunk, b.items[i:end])

		g.Go(func(ctx context.Context) error {
			return b.processor(ctx, chunk)
		})
	}

	return g.WaitAll()
}

// Clear removes all pending items.
func (b *Batch[T]) Clear() {
	b.items = b.items[:0]
}

// Len returns the number of pending items.
func (b *Batch[T]) Len() int {
	return len(b.items)
}
