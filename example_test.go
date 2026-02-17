package worker_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

func ExampleNewPool() {
	pool, err := worker.NewPool(
		worker.WithWorkers(4),
		worker.WithQueueSize(100),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	_ = pool.Submit(func(ctx context.Context) error {
		fmt.Println("Hello from worker!")
		return nil
	})

	pool.Close()
	// Output: Hello from worker!
}

func ExamplePool_SubmitWait() {
	pool, _ := worker.NewPool()
	defer pool.Close()

	err := pool.SubmitWait(context.Background(), func(ctx context.Context) error {
		// Do work...
		return nil
	})
	fmt.Println("error:", err)
	// Output: error: <nil>
}

func ExampleSubmitTyped() {
	pool, _ := worker.NewPool()
	defer pool.Close()

	future := worker.SubmitTyped(pool, func(ctx context.Context) (int, error) {
		return 42, nil
	})

	val, err := future.Await(context.Background())
	fmt.Printf("val=%d err=%v\n", val, err)
	// Output: val=42 err=<nil>
}

func ExampleErrorGroup() {
	pool, _ := worker.NewPool(worker.WithWorkers(4))
	defer pool.Close()

	g := worker.NewErrorGroup(pool)

	g.Go(func(ctx context.Context) error {
		// Task 1
		return nil
	})
	g.Go(func(ctx context.Context) error {
		// Task 2
		return nil
	})

	if err := g.Wait(); err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println("all tasks completed")
	// Output: all tasks completed
}

func ExampleBatch() {
	pool, _ := worker.NewPool(worker.WithWorkers(4))
	defer pool.Close()

	batch := worker.NewBatch(pool, 3, func(ctx context.Context, items []int) error {
		fmt.Printf("processing batch of %d items\n", len(items))
		return nil
	})

	for i := range 7 {
		batch.Add(i)
	}
	_ = batch.Process(context.Background())
}

func ExampleNewPool_withRetries() {
	pool, _ := worker.NewPool(
		worker.WithWorkers(2),
		worker.WithMaxRetries(3),
		worker.WithRetryDelay(100*time.Millisecond),
		worker.WithRetryBackoff(true),
		worker.WithPanicHandler(func(job worker.Job, r any) {
			log.Printf("job panicked: %v", r)
		}),
	)
	defer pool.Close()

	_ = pool.Submit(func(ctx context.Context) error {
		// This will be retried up to 3 times on failure
		return nil
	})
}

func ExampleNewPool_withMiddleware() {
	logging := func(next func(ctx context.Context) error) func(ctx context.Context) error {
		return func(ctx context.Context) error {
			start := time.Now()
			err := next(ctx)
			log.Printf("job took %v, err=%v", time.Since(start), err)
			return err
		}
	}

	pool, _ := worker.NewPool(
		worker.WithMiddleware(logging),
	)
	defer pool.Close()
}
