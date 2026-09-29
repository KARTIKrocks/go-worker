package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Sentinel errors returned by [RateLimiter].
var (
	ErrRateLimited    = errors.New("worker: rate limit exceeded")
	ErrLimiterStopped = errors.New("worker: rate limiter stopped")
)

// RateLimiter is a token-bucket rate limiter that wraps a worker pool.
type RateLimiter struct {
	pool   *Pool
	tokens chan struct{}
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewRateLimiter creates a rate limiter that allows n jobs per interval.
// It panics if n < 1 or interval <= 0.
func NewRateLimiter(pool *Pool, n int, interval time.Duration) *RateLimiter {
	if n < 1 || interval <= 0 {
		panic(fmt.Sprintf("worker: NewRateLimiter: need n >= 1 and interval > 0, got n=%d interval=%v", n, interval))
	}
	ctx, cancel := context.WithCancel(context.Background())
	rl := &RateLimiter{
		pool:   pool,
		tokens: make(chan struct{}, n),
		ctx:    ctx,
		cancel: cancel,
	}
	for range n {
		rl.tokens <- struct{}{}
	}
	rl.wg.Add(1)
	go rl.refill(n, interval)
	return rl
}

func (rl *RateLimiter) refill(n int, interval time.Duration) {
	defer rl.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-rl.ctx.Done():
			return
		case <-ticker.C:
			for range n {
				rl.putToken()
			}
		}
	}
}

// putToken returns a token to the bucket unless it is already full.
func (rl *RateLimiter) putToken() {
	select {
	case rl.tokens <- struct{}{}:
	default:
	}
}

// Submit waits for a token and then submits the job. Respects the provided
// context. Returns [ErrLimiterStopped] once [RateLimiter.Stop] has been called.
// If the pool rejects the job, the token is returned.
func (rl *RateLimiter) Submit(ctx context.Context, job Job) error {
	if rl.ctx.Err() != nil {
		return ErrLimiterStopped
	}
	select {
	case <-rl.tokens:
		return rl.spend(func() error { return rl.pool.SubmitContext(ctx, job) })
	case <-ctx.Done():
		return ctx.Err()
	case <-rl.ctx.Done():
		return ErrLimiterStopped
	}
}

// TrySubmit attempts to submit without waiting for a token. It returns
// [ErrRateLimited] if no token is available, or the pool's error (such as
// [ErrPoolFull]) if the pool rejects the job, in which case the token is returned.
func (rl *RateLimiter) TrySubmit(job Job) error {
	if rl.ctx.Err() != nil {
		return ErrLimiterStopped
	}
	select {
	case <-rl.tokens:
		return rl.spend(func() error { return rl.pool.TrySubmitJob(job) })
	default:
		return ErrRateLimited
	}
}

// spend uses a token the caller has just taken to run submit. Stop may have
// run since the caller's first check (and select picks randomly between a
// token and rl.ctx.Done), so re-check before submitting. The token is
// returned if the limiter is stopped or submit fails.
func (rl *RateLimiter) spend(submit func() error) error {
	if rl.ctx.Err() != nil {
		rl.putToken()
		return ErrLimiterStopped
	}
	if err := submit(); err != nil {
		rl.putToken()
		return err
	}
	return nil
}

// Stop stops the token refiller. Subsequent submits return [ErrLimiterStopped].
func (rl *RateLimiter) Stop() {
	rl.cancel()
	rl.wg.Wait()
}
