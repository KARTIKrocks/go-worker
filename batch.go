package worker

import (
	"context"
	"iter"
	"slices"
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
	for chunk := range b.chunks() {
		g.Go(func(ctx context.Context) error { return b.processor(ctx, chunk) })
	}
	return g.Wait()
}

// ProcessAll processes all items and collects all errors (does not cancel on first error).
func (b *Batch[T]) ProcessAll(ctx context.Context) []error {
	if len(b.items) == 0 {
		return nil
	}

	g := NewGroupContext(ctx, b.pool)
	for chunk := range b.chunks() {
		g.Go(func(ctx context.Context) error { return b.processor(ctx, chunk) })
	}
	return g.WaitAll()
}

// chunks yields the pending items in pieces of up to batchSize. Each piece is
// a copy, so the processor never shares memory with b.items.
func (b *Batch[T]) chunks() iter.Seq[[]T] {
	return func(yield func([]T) bool) {
		for chunk := range slices.Chunk(b.items, b.batchSize) {
			if !yield(slices.Clone(chunk)) {
				return
			}
		}
	}
}

// Clear removes all pending items.
func (b *Batch[T]) Clear() {
	b.items = b.items[:0]
}

// Len returns the number of pending items.
func (b *Batch[T]) Len() int {
	return len(b.items)
}
