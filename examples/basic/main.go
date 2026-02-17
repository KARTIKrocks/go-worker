// Basic demonstrates the core worker pool functionality:
// fire-and-forget, wait-for-result, non-blocking, and context-aware submission.
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
	pool, err := worker.NewPool(
		worker.WithWorkers(4),
		worker.WithQueueSize(100),
		worker.WithJobTimeout(5*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// --- Fire and forget ---
	var count atomic.Int32
	for i := range 10 {
		if err := pool.Submit(func(ctx context.Context) error {
			count.Add(1)
			fmt.Printf("  job %d done\n", i)
			return nil
		}); err != nil {
			log.Fatal(err)
		}
	}

	// --- Wait for result ---
	err = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		time.Sleep(50 * time.Millisecond)
		return nil
	})
	fmt.Println("SubmitWait returned:", err)

	// --- Non-blocking (TrySubmit) ---
	err = pool.TrySubmit(func(ctx context.Context) error {
		fmt.Println("  TrySubmit job ran")
		return nil
	})
	if err != nil {
		fmt.Println("TrySubmit error:", err) // ErrPoolFull if queue is full
	}

	// --- Context-aware submission ---
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	job := worker.JobFunc(func(ctx context.Context) error {
		fmt.Println("  SubmitContext job ran")
		return nil
	})
	err = pool.SubmitContext(ctx, job)
	fmt.Println("SubmitContext returned:", err)

	// --- Pause / Resume ---
	pool.Pause()
	fmt.Println("Pool paused:", pool.IsPaused())

	pool.Resume()
	fmt.Println("Pool resumed:", !pool.IsPaused())

	// --- Metrics ---
	pool.Close()
	snap := pool.Snapshot()
	fmt.Printf("\nMetrics:\n")
	fmt.Printf("  Submitted:  %d\n", snap.JobsSubmitted)
	fmt.Printf("  Completed:  %d\n", snap.JobsCompleted)
	fmt.Printf("  Failed:     %d\n", snap.JobsFailed)
	fmt.Printf("  Success:    %.0f%%\n", snap.SuccessRate()*100)
	fmt.Printf("  Throughput: %.1f jobs/s\n", snap.Throughput())
}
