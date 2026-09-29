package worker

import (
	"context"
	"sync"
)

// groupBase is the machinery shared by [Group] and [ErrorGroup]: it runs
// functions on a pool under a shared cancellable context and waits for them.
type groupBase struct {
	pool   *Pool
	wg     sync.WaitGroup
	ctx    context.Context
	cancel context.CancelFunc
}

func (b *groupBase) init(ctx context.Context, pool *Pool) {
	b.pool = pool
	b.ctx, b.cancel = context.WithCancel(ctx)
}

// run submits fn and calls done exactly once with its final result: the pool
// reports it once per job, after all retries, so the WaitGroup stays balanced.
// fn is skipped if the group is already cancelled when it would start.
func (b *groupBase) run(fn func(ctx context.Context) error, done func(error)) {
	b.wg.Add(1)
	finish := func(err error) {
		done(err)
		b.wg.Done()
	}
	job := JobFunc(func(ctx context.Context) error {
		// The job context derives from the group context.
		if err := ctx.Err(); err != nil {
			return err
		}
		return fn(ctx)
	})
	if err := b.pool.submit(b.ctx, b.ctx, job, finish, true); err != nil {
		finish(err)
	}
}

// wait blocks until every function has finished, then cancels the group context.
func (b *groupBase) wait() {
	b.wg.Wait()
	b.cancel()
}

// Group runs multiple jobs concurrently on a pool and collects results.
// Unlike [ErrorGroup], it does not cancel on the first error.
type Group struct {
	groupBase
	mu     sync.Mutex
	errors []error
}

// NewGroup creates a job group using context.Background.
func NewGroup(pool *Pool) *Group {
	return NewGroupContext(context.Background(), pool)
}

// NewGroupContext creates a job group bound to a parent context.
func NewGroupContext(ctx context.Context, pool *Pool) *Group {
	g := &Group{}
	g.init(ctx, pool)
	return g
}

// Go submits a function to the group for concurrent execution.
// The function's context is cancelled when the group's context is, when the
// pool's job timeout expires, or when the pool is force-closed.
func (g *Group) Go(fn func(ctx context.Context) error) {
	g.run(fn, g.record)
}

func (g *Group) record(err error) {
	if err != nil {
		g.mu.Lock()
		g.errors = append(g.errors, err)
		g.mu.Unlock()
	}
}

// Wait blocks until all submitted functions complete and returns the first error.
func (g *Group) Wait() error {
	if errs := g.WaitAll(); len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// WaitAll blocks until all submitted functions complete and returns all errors.
func (g *Group) WaitAll() []error {
	g.wait()
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.errors
}

// ErrorGroup runs multiple jobs concurrently and cancels the group context
// on the first error. Modeled after [golang.org/x/sync/errgroup].
type ErrorGroup struct {
	groupBase
	errOnce sync.Once
	err     error
}

// NewErrorGroup creates an error group using context.Background.
func NewErrorGroup(pool *Pool) *ErrorGroup {
	return NewErrorGroupContext(context.Background(), pool)
}

// NewErrorGroupContext creates an error group bound to a parent context.
// The derived context is cancelled when the first function returns an error
// or when [ErrorGroup.Wait] returns.
func NewErrorGroupContext(ctx context.Context, pool *Pool) *ErrorGroup {
	g := &ErrorGroup{}
	g.init(ctx, pool)
	return g
}

// Go submits a function to the error group. If fn returns an error, the group
// context is cancelled, signaling other functions to stop.
func (g *ErrorGroup) Go(fn func(ctx context.Context) error) {
	g.run(fn, g.record)
}

func (g *ErrorGroup) record(err error) {
	if err != nil {
		g.errOnce.Do(func() {
			g.err = err
			g.cancel()
		})
	}
}

// Wait blocks until all functions complete and returns the first error, if any.
func (g *ErrorGroup) Wait() error {
	g.wait()
	return g.err
}

// Context returns the group's derived context. It is cancelled on the first
// error or when Wait returns.
func (g *ErrorGroup) Context() context.Context {
	return g.ctx
}
