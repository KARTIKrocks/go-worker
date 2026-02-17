// Graceful-shutdown demonstrates Close, CloseWithTimeout, and
// context cancellation behavior during pool shutdown.
package main

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

func main() {
	// --- Graceful close: waits for all jobs ---
	fmt.Println("=== Graceful Close ===")
	gracefulDemo()

	// --- CloseWithTimeout: force-cancel after deadline ---
	fmt.Println("\n=== CloseWithTimeout ===")
	timeoutDemo()
}

func gracefulDemo() {
	pool, err := worker.NewPool(
		worker.WithWorkers(2),
		worker.WithQueueSize(10),
		worker.WithJobTimeout(0),
	)
	if err != nil {
		log.Fatal(err)
	}

	var completed atomic.Int32

	// Submit several slow jobs
	for i := range 5 {
		_ = pool.Submit(func(ctx context.Context) error {
			time.Sleep(50 * time.Millisecond)
			n := completed.Add(1)
			fmt.Printf("  job %d completed (%d total)\n", i, n)
			return nil
		})
	}

	// Close waits for all 5 to finish
	start := time.Now()
	pool.Close()
	fmt.Printf("  pool closed after %v, completed: %d\n",
		time.Since(start).Round(time.Millisecond), completed.Load())

	// Submitting after close returns ErrPoolClosed
	err = pool.Submit(func(ctx context.Context) error { return nil })
	fmt.Printf("  submit after close: %v\n", err)
}

func timeoutDemo() {
	pool, err := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithJobTimeout(0), // no per-job timeout
	)
	if err != nil {
		log.Fatal(err)
	}

	// Submit a long-running job that respects context cancellation
	_ = pool.Submit(func(ctx context.Context) error {
		fmt.Println("  long job started, waiting for cancellation...")
		<-ctx.Done()
		fmt.Println("  long job cancelled:", ctx.Err())
		return ctx.Err()
	})

	time.Sleep(50 * time.Millisecond) // let it start

	// CloseWithTimeout: give 100ms, then force-cancel
	start := time.Now()
	pool.CloseWithTimeout(100 * time.Millisecond)
	fmt.Printf("  pool force-closed after %v\n", time.Since(start).Round(time.Millisecond))
}
