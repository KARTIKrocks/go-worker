// ErrorGroup demonstrates fan-out/fan-in patterns using ErrorGroup
// (cancel on first error) and Group (collect all errors).
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

func main() {
	pool, err := worker.NewPool(worker.WithWorkers(4), worker.WithJobTimeout(0))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// --- ErrorGroup: cancel on first error ---
	fmt.Println("=== ErrorGroup (cancel on first error) ===")

	g := worker.NewErrorGroupContext(context.Background(), pool)

	g.Go(func(ctx context.Context) error {
		time.Sleep(100 * time.Millisecond)
		fmt.Println("  task A: completed")
		return nil
	})

	g.Go(func(ctx context.Context) error {
		return errors.New("task B failed")
	})

	g.Go(func(ctx context.Context) error {
		// This task observes the group cancellation from task B's error.
		<-ctx.Done()
		fmt.Println("  task C: cancelled due to", ctx.Err())
		return ctx.Err()
	})

	err = g.Wait()
	fmt.Println("ErrorGroup result:", err)

	// --- Group: collect all errors ---
	fmt.Println("\n=== Group (collect all errors) ===")

	g2 := worker.NewGroup(pool)

	for i := range 5 {
		g2.Go(func(ctx context.Context) error {
			if i%2 == 0 {
				return fmt.Errorf("task %d failed", i)
			}
			fmt.Printf("  task %d: success\n", i)
			return nil
		})
	}

	errs := g2.WaitAll()
	fmt.Printf("Group collected %d errors:\n", len(errs))
	for _, e := range errs {
		fmt.Printf("  - %v\n", e)
	}
}
