---
id: batch
title: Batch
description: Process large slices in fixed-size chunks on a go-worker pool with the generic Batch type — bulk inserts, bulk API calls — with fail-fast or collect-all error handling.
---

# Batch

`Batch[T]` splits a set of items into chunks and processes the chunks in
parallel on the pool. It fits bulk operations where the downstream system
prefers a few large calls over many small ones — bulk database inserts,
batch APIs, multi-key cache writes.

```go
batch := worker.NewBatch(pool, 500, func(ctx context.Context, users []User) error {
    return db.BulkInsert(ctx, users)
})

batch.Add(users...)

if err := batch.Process(ctx); err != nil {
    return err
}
```

The processor is called once per chunk of up to `batchSize` items; the last
chunk holds whatever is left. Each chunk is a new slice, so the processor may keep
it or reorder it freely. The copy is shallow: items holding pointers, maps, or
slices still share that data with the originals.

## Fail fast or collect everything

| Method | Runs chunks with | On a failed chunk | Returns |
| --- | --- | --- | --- |
| `Process(ctx)` | an [`ErrorGroup`](./groups.md#errorgroup) | cancels the remaining chunks | the first error |
| `ProcessAll(ctx)` | a [`Group`](./groups.md#group) | keeps going | every error |

Use `Process` when one failure should stop the rest. It is not
a transaction: chunks that already ran, or are running when the context is
cancelled, keep their side effects. Use `ProcessAll` when chunks
are independent and you want to report or retry the ones that failed.

Chunks run concurrently, so they can finish in any order. If order matters —
an append-only log — process sequentially instead.

## Managing items

```go
batch.Add(more...) // append items
batch.Len()        // items pending
batch.Clear()      // remove all pending items
```

Processing does **not** remove items. Calling `Process` twice processes the
same items twice; call `Clear` in between to reuse a batch.

`Batch` is not safe for concurrent use. Do not call `Add` from one goroutine
while another runs `Process`.

## Choosing a batch size

Pick it from the downstream limit, not from the item count: the most rows your
database takes comfortably in one insert, or the maximum size a batch API
accepts. The pool's worker count then bounds how many of those big calls run at
once. A `batchSize` below 1 is treated as 1.
