// Rate-limiter demonstrates the Ticker, Debouncer, Throttler, and RateLimiter helpers.
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
	pool, err := worker.NewPool(worker.WithWorkers(4), worker.WithJobTimeout(0))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// --- Ticker: fixed-interval job submission ---
	fmt.Println("=== Ticker ===")
	var ticks atomic.Int32
	ticker := worker.NewTickerFunc(pool, 100*time.Millisecond, func(ctx context.Context) error {
		fmt.Printf("  tick #%d\n", ticks.Add(1))
		return nil
	})
	ticker.Start()
	ticker.Start() // idempotent — safe to call twice
	time.Sleep(350 * time.Millisecond)
	ticker.Stop()
	fmt.Printf("  total ticks: %d\n\n", ticks.Load())

	// --- Debouncer: collapse rapid calls ---
	fmt.Println("=== Debouncer (trailing edge) ===")
	debouncer := worker.NewDebouncer(pool, 200*time.Millisecond)

	// Rapid submissions — only the last one fires
	for i := range 5 {
		debouncer.SubmitFunc(func(ctx context.Context) error {
			fmt.Printf("  debounced call (i=%d)\n", i)
			return nil
		})
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond) // wait for debounce to fire
	fmt.Println()

	// --- Leading-edge debouncer ---
	fmt.Println("=== Debouncer (leading edge) ===")
	leading := worker.NewDebouncerLeading(pool, 200*time.Millisecond)

	for i := range 3 {
		leading.SubmitFunc(func(ctx context.Context) error {
			fmt.Printf("  leading debounce (i=%d)\n", i)
			return nil
		})
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	fmt.Println()

	// --- Throttler: at most one per interval ---
	fmt.Println("=== Throttler ===")
	throttler := worker.NewThrottler(pool, 200*time.Millisecond)

	for i := range 5 {
		ok := throttler.Submit(worker.JobFunc(func(ctx context.Context) error {
			fmt.Printf("  throttled job (i=%d)\n", i)
			return nil
		}))
		fmt.Printf("  submit i=%d accepted=%v\n", i, ok)
		time.Sleep(80 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	fmt.Println()

	// --- RateLimiter: token bucket ---
	fmt.Println("=== RateLimiter (3 tokens / 200ms) ===")
	rl := worker.NewRateLimiter(pool, 3, 200*time.Millisecond)
	defer rl.Stop()

	ctx := context.Background()
	start := time.Now()

	for i := range 6 {
		err := rl.Submit(ctx, worker.JobFunc(func(ctx context.Context) error {
			elapsed := time.Since(start).Round(time.Millisecond)
			fmt.Printf("  rate-limited job %d at %v\n", i, elapsed)
			return nil
		}))
		if err != nil {
			fmt.Printf("  job %d error: %v\n", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond) // let jobs finish
}
