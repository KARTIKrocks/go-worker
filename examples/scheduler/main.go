// Scheduler demonstrates periodic task execution using cron expressions,
// fixed intervals, and runtime control (pause, resume, trigger, remove).
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

	sched := worker.NewScheduler(pool,
		worker.WithTickInterval(50*time.Millisecond), // fast tick for demo
		worker.WithOnTaskStart(func(name string) {
			fmt.Printf("  [start] %s\n", name)
		}),
		worker.WithOnTaskEnd(func(name string, err error, d time.Duration) {
			fmt.Printf("  [end]   %s (took %v, err=%v)\n", name, d.Round(time.Millisecond), err)
		}),
	)

	// --- Fixed interval with immediate first run ---
	var heartbeats atomic.Int32
	sched.EveryFunc("heartbeat", 200*time.Millisecond, func(ctx context.Context) error {
		n := heartbeats.Add(1)
		fmt.Printf("    heartbeat #%d\n", n)
		return nil
	}, worker.WithRunImmediate())

	// --- One-shot task ---
	sched.OnceFunc("init", time.Now().Add(100*time.Millisecond), func(ctx context.Context) error {
		fmt.Println("    one-shot initialization done")
		return nil
	})

	// --- Interval with jitter and max runs ---
	sched.EveryFunc("sync", 150*time.Millisecond, func(ctx context.Context) error {
		fmt.Println("    syncing data...")
		return nil
	},
		worker.WithJitter(50*time.Millisecond),
		worker.WithMaxRuns(3),
		worker.WithOverlapPolicy(worker.OverlapSkip),
	)

	// --- Start and let it run ---
	fmt.Println("=== Scheduler running ===")
	sched.Start()
	time.Sleep(800 * time.Millisecond)

	// --- Runtime control ---
	fmt.Println("\n=== Pausing heartbeat ===")
	sched.Pause("heartbeat")
	time.Sleep(300 * time.Millisecond)

	fmt.Println("\n=== Resuming heartbeat ===")
	sched.Resume("heartbeat")
	time.Sleep(300 * time.Millisecond)

	// --- Manual trigger ---
	fmt.Println("\n=== Manual trigger ===")
	sched.Trigger("heartbeat")
	time.Sleep(100 * time.Millisecond)

	// --- Task info ---
	if info := sched.TaskInfo("heartbeat"); info != nil {
		fmt.Printf("\nHeartbeat info: runs=%d, success=%.0f%%\n",
			info.RunCount, info.SuccessRate()*100)
	}

	// --- List and remove ---
	fmt.Println("\nRegistered tasks:", sched.Tasks())
	sched.Remove("heartbeat")
	fmt.Println("After remove:", sched.Tasks())

	sched.Stop()
	fmt.Println("\nScheduler stopped. Total heartbeats:", heartbeats.Load())
}
