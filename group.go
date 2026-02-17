package worker

import (
	"context"
	"sync"
)

// Group runs multiple jobs concurrently on a pool and collects results.
// Unlike [ErrorGroup], it does not cancel on the first error.
type Group struct {
	pool   *Pool
	wg     sync.WaitGroup
	mu     sync.Mutex
	errors []error
	ctx    context.Context
	cancel context.CancelFunc
}

// NewGroup creates a job group using context.Background.
func NewGroup(pool *Pool) *Group {
	return NewGroupContext(context.Background(), pool)
}

// NewGroupContext creates a job group bound to a parent context.
func NewGroupContext(ctx context.Context, pool *Pool) *Group {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{
		pool:   pool,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Go submits a function to the group for concurrent execution.
// The function receives the group's context, so group-level cancellation is visible.
func (g *Group) Go(fn func(ctx context.Context) error) {
	g.wg.Add(1)

	job := JobFunc(func(_ context.Context) error {
		defer g.wg.Done()
		if err := g.ctx.Err(); err != nil {
			return err
		}
		if err := fn(g.ctx); err != nil {
			g.mu.Lock()
			g.errors = append(g.errors, err)
			g.mu.Unlock()
			return err
		}
		return nil
	})

	if err := g.pool.SubmitContext(g.ctx, job); err != nil {
		g.wg.Done()
		g.mu.Lock()
		g.errors = append(g.errors, err)
		g.mu.Unlock()
	}
}

// Wait blocks until all submitted functions complete and returns the first error.
func (g *Group) Wait() error {
	g.wg.Wait()
	g.cancel()
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.errors) > 0 {
		return g.errors[0]
	}
	return nil
}

// WaitAll blocks until all submitted functions complete and returns all errors.
func (g *Group) WaitAll() []error {
	g.wg.Wait()
	g.cancel()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.errors
}

// ErrorGroup runs multiple jobs concurrently and cancels the group context
// on the first error. Modeled after [golang.org/x/sync/errgroup].
type ErrorGroup struct {
	pool    *Pool
	wg      sync.WaitGroup
	errOnce sync.Once
	err     error
	ctx     context.Context
	cancel  context.CancelFunc
}

// NewErrorGroup creates an error group using context.Background.
func NewErrorGroup(pool *Pool) *ErrorGroup {
	return NewErrorGroupContext(context.Background(), pool)
}

// NewErrorGroupContext creates an error group bound to a parent context.
// The derived context is cancelled when the first function returns an error
// or when [ErrorGroup.Wait] returns.
func NewErrorGroupContext(ctx context.Context, pool *Pool) *ErrorGroup {
	ctx, cancel := context.WithCancel(ctx)
	return &ErrorGroup{
		pool:   pool,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Go submits a function to the error group. If fn returns an error, the group
// context is cancelled, signaling other functions to stop.
func (g *ErrorGroup) Go(fn func(ctx context.Context) error) {
	g.wg.Add(1)

	job := JobFunc(func(ctx context.Context) error {
		defer g.wg.Done()
		if err := g.ctx.Err(); err != nil {
			return err
		}
		if err := fn(g.ctx); err != nil {
			g.errOnce.Do(func() {
				g.err = err
				g.cancel()
			})
			return err
		}
		return nil
	})

	if err := g.pool.SubmitContext(g.ctx, job); err != nil {
		g.wg.Done()
		g.errOnce.Do(func() {
			g.err = err
			g.cancel()
		})
	}
}

// Wait blocks until all functions complete and returns the first error, if any.
func (g *ErrorGroup) Wait() error {
	g.wg.Wait()
	g.cancel()
	return g.err
}

// Context returns the group's derived context. It is cancelled on the first
// error or when Wait returns.
func (g *ErrorGroup) Context() context.Context {
	return g.ctx
}
