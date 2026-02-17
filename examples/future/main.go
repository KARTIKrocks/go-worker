// Future demonstrates generic typed async results using SubmitTyped and Future[T].
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/KARTIKrocks/go-worker"
)

type Quote struct {
	Symbol string
	Price  float64
}

func fetchQuote(symbol string) func(ctx context.Context) (Quote, error) {
	return func(ctx context.Context) (Quote, error) {
		// Simulate API call
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		return Quote{Symbol: symbol, Price: 100 + rand.Float64()*50}, nil
	}
}

func main() {
	pool, err := worker.NewPool(worker.WithWorkers(4), worker.WithJobTimeout(0))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	// --- Submit multiple typed tasks concurrently ---
	symbols := []string{"AAPL", "GOOG", "MSFT", "AMZN"}
	futures := make([]*worker.Future[Quote], len(symbols))

	for i, sym := range symbols {
		futures[i] = worker.SubmitTyped(pool, fetchQuote(sym))
	}

	// --- Check if done (non-blocking) ---
	fmt.Println("Checking if first future is done:", futures[0].Done())

	// --- Await all results ---
	ctx := context.Background()
	fmt.Println("\nStock Quotes:")
	for _, f := range futures {
		quote, err := f.Await(ctx)
		if err != nil {
			fmt.Printf("  error: %v\n", err)
			continue
		}
		fmt.Printf("  %s: $%.2f\n", quote.Symbol, quote.Price)
	}

	// --- Await with timeout ---
	fmt.Println("\n=== Await with timeout ===")
	slowFuture := worker.SubmitTyped(pool, func(ctx context.Context) (string, error) {
		time.Sleep(500 * time.Millisecond)
		return "done", nil
	})

	shortCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	val, err := slowFuture.Await(shortCtx)
	fmt.Printf("Result: %q, err: %v\n", val, err) // context.DeadlineExceeded
}
