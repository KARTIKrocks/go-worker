// Retries demonstrates automatic retries with exponential backoff,
// max retry delay cap, and panic recovery.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

func main() {
	pool, err := worker.NewPool(
		worker.WithWorkers(2),
		worker.WithMaxRetries(3),
		worker.WithRetryDelay(100*time.Millisecond),
		worker.WithRetryBackoff(true),
		worker.WithMaxRetryDelay(500*time.Millisecond), // cap backoff at 500ms
		worker.WithPanicHandler(func(job worker.Job, r any) {
			fmt.Printf("  panic recovered: %v\n", r)
		}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// --- Retry until success ---
	fmt.Println("=== Retry until success ===")
	var attempts atomic.Int32

	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		n := attempts.Add(1)
		if n < 3 {
			fmt.Printf("  attempt %d: failing\n", n)
			return errors.New("transient error")
		}
		fmt.Printf("  attempt %d: success!\n", n)
		return nil
	})
	fmt.Println("Result:", err)

	// --- All retries exhausted ---
	fmt.Println("\n=== Retries exhausted ===")
	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		return errors.New("persistent failure")
	})
	fmt.Println("Result:", err) // will be "persistent failure"

	// --- Panic recovery ---
	fmt.Println("\n=== Panic recovery ===")
	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		panic("something went wrong")
	})
	fmt.Println("Result:", errors.Is(err, worker.ErrJobPanic))

	// Worker is still alive after panic
	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		fmt.Println("  pool still works after panic!")
		return nil
	})
	fmt.Println("Result:", err)
}
