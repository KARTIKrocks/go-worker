// Batch demonstrates bulk processing of items in configurable-sized chunks.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"

	"github.com/KARTIKrocks/go-worker"
)

type User struct {
	ID   int
	Name string
}

func main() {
	pool, err := worker.NewPool(worker.WithWorkers(4), worker.WithJobTimeout(0))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// --- Batch with cancel-on-first-error ---
	fmt.Println("=== Batch.Process (cancel on first error) ===")

	var inserted atomic.Int32

	batch := worker.NewBatch(pool, 3, func(ctx context.Context, users []User) error {
		// Simulate bulk insert
		inserted.Add(int32(len(users)))
		fmt.Printf("  inserted batch of %d users (IDs: ", len(users))
		for i, u := range users {
			if i > 0 {
				fmt.Print(", ")
			}
			fmt.Print(u.ID)
		}
		fmt.Println(")")
		return nil
	})

	// Add 10 users — they'll be processed in chunks of 3
	for i := 1; i <= 10; i++ {
		batch.Add(User{ID: i, Name: fmt.Sprintf("user_%d", i)})
	}

	fmt.Printf("Pending items: %d\n", batch.Len())

	if err := batch.Process(context.Background()); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Total inserted: %d\n", inserted.Load())

	// --- Batch.ProcessAll (collect all errors) ---
	fmt.Println("\n=== Batch.ProcessAll (collect all errors) ===")

	batch2 := worker.NewBatch(pool, 2, func(ctx context.Context, nums []int) error {
		for _, n := range nums {
			if n == 5 {
				return fmt.Errorf("cannot process item %d", n)
			}
		}
		fmt.Printf("  processed: %v\n", nums)
		return nil
	})

	for i := 1; i <= 8; i++ {
		batch2.Add(i)
	}

	errs := batch2.ProcessAll(context.Background())
	if len(errs) > 0 {
		fmt.Printf("Errors (%d):\n", len(errs))
		for _, e := range errs {
			fmt.Printf("  - %v\n", e)
		}
	}
}
