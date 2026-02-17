// Middleware demonstrates composable middleware chains for cross-cutting
// concerns like logging, timing, and tracing.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

// timingMiddleware logs how long each job takes.
func timingMiddleware(next func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		start := time.Now()
		err := next(ctx)
		fmt.Printf("  [timing] job took %v, err=%v\n", time.Since(start).Round(time.Millisecond), err)
		return err
	}
}

// retryTagMiddleware adds a tag to the context showing it passed through middleware.
func retryTagMiddleware(next func(ctx context.Context) error) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		fmt.Println("  [tag] before job")
		err := next(ctx)
		fmt.Println("  [tag] after job")
		return err
	}
}

func main() {
	// Middleware is applied in order: timingMiddleware wraps retryTagMiddleware wraps the job.
	pool, err := worker.NewPool(
		worker.WithWorkers(2),
		worker.WithMiddleware(timingMiddleware, retryTagMiddleware),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	fmt.Println("=== Job 1 (success) ===")
	_ = pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		time.Sleep(25 * time.Millisecond)
		fmt.Println("  [job] doing work")
		return nil
	})

	fmt.Println("\n=== Job 2 (with hooks) ===")

	// Hooks complement middleware for observability.
	pool2, _ := worker.NewPool(
		worker.WithWorkers(1),
		worker.WithHooks(worker.Hooks{
			OnJobStart: func(j worker.Job) {
				fmt.Println("  [hook] job started")
			},
			OnJobComplete: func(j worker.Job, d time.Duration) {
				fmt.Printf("  [hook] job completed in %v\n", d.Round(time.Millisecond))
			},
			OnWorkerStart: func(id int) {
				fmt.Printf("  [hook] worker %d started\n", id)
			},
		}),
	)
	defer pool2.Close()

	_ = pool2.SubmitWait(context.Background(), func(ctx context.Context) error {
		fmt.Println("  [job] working...")
		return nil
	})
}
