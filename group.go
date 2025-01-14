package parallel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/outofforest/logger"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var nextTaskID int64 = 0x0bace1d000000000

// Group is a facility for running a task with several subtasks without
// inversion of control. For most ordinary use cases, use Run instead.
//
//	return Run(ctx, start)
//
// ...is equivalent to:
//
//	g := NewGroup(ctx)
//	if err := start(g.Context(), g.Spawn); err != nil {
//	    g.Exit(err)
//	}
//	return g.Wait()
//
// Group is mostly useful in test suites where starting and finishing the group
// is controlled by test setup and teardown functions.
type Group struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	running int
	done    chan struct{}
	closing bool
	err     error
}

// NewGroup creates a new Group controlled by the given context
func NewGroup(ctx context.Context) *Group {
	g := new(Group)
	g.ctx, g.cancel = context.WithCancel(ctx)
	g.done = make(chan struct{})
	close(g.done)
	return g
}

// NewSubgroup creates a new Group nested within another. The spawn argument is
// the spawn function of the parent group.
//
// The subgroup's context is inherited from the parent group. The entire
// subgroup is treated as a task in the parent group.
//
// Example within parallel.Run:
//
//	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
//	    spawn(...)
//	    spawn(...)
//	    subgroup := parallel.NewSubgroup(spawn, "updater")
//	    subgroup.Spawn(...)
//	    subgroup.Spawn(...)
//	    return nil
//	})
//
// Example within an explicit group:
//
//	group := parallel.NewGroup(ctx)
//	group.Spawn(...)
//	group.Spawn(...)
//	subgroup := parallel.NewSubgroup(group.Spawn, "updater")
//	subgroup.Spawn(...)
//	subgroup.Spawn(...)
func NewSubgroup(spawn SpawnFn, name string, onExit OnExit, fields ...zapcore.Field) *Group {
	ch := make(chan *Group)
	spawn(name, onExit, func(ctx context.Context) error {
		if len(fields) > 0 {
			ctx = logger.With(ctx, fields...)
		}
		g := NewGroup(ctx)
		ch <- g
		return g.Complete(ctx)
	})
	return <-ch
}

// Context returns the inner context of the group which controls the lifespan of
// its subtasks
func (g *Group) Context() context.Context {
	return g.ctx
}

// Spawn spawns a subtask. See documentation for SpawnFn.
//
// When a subtask finishes, it sets the result of the group if it's not already
// set (unless the task returns nil and its OnExit mode is Continue).
func (g *Group) Spawn(name string, onExit OnExit, task Task) {
	id := atomic.AddInt64(&nextTaskID, 1)

	g.mu.Lock()
	if g.running == 0 {
		g.done = make(chan struct{})
	}
	g.running++
	g.mu.Unlock()

	log := logger.Get(g.ctx).Named(name)
	log.Debug("Task spawned", zap.String("id", fmt.Sprintf("%x", id)), zap.Stringer("onExit", onExit))

	go g.runTask(logger.WithLogger(g.ctx, log), id, name, onExit, task)
}

// Second parameter is the task ID. It is ignored because the only reason to
// pass it is to add it to the stack trace
func (g *Group) runTask(ctx context.Context, _ int64, name string, onExit OnExit, task Task) {
	err := runTask(ctx, task)
	logger.Get(ctx).Debug("Task finished", zap.Error(err))

	g.mu.Lock()
	defer g.mu.Unlock()

	if err != nil {
		g.exit(err)
	} else if !g.closing {
		switch onExit {
		case Continue:
		case Exit:
			g.exit(nil)
		case Fail:
			g.exit(errors.Errorf("task %s terminated unexpectedly", name))
		default:
			g.exit(errors.Errorf("task %s: %v", name, onExit))
		}
	}

	g.running--
	if g.running == 0 {
		close(g.done)
	}
}

func (g *Group) exit(err error) {
	// Cancellations during shutdown are fine
	if g.closing && errors.Is(err, context.Canceled) {
		return
	}
	if g.err == nil {
		g.err = err
	}
	if !g.closing {
		g.closing = true
		g.cancel()
	}
}

// Exit prompts the group to shut down, if it's not already shutting down or
// finished. This causes the inner context to close, which should prompt any
// running subtasks to exit. Use Wait to block until all the subtasks actually
// finish.
//
// If the group result is not yet set, Exit sets it to err.
func (g *Group) Exit(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	g.exit(err)
}

// Running returns the number of running subtasks
func (g *Group) Running() int {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.running
}

// Done returns a channel that closes when the last running subtask finishes. If
// no subtasks are running, the returned channel is already closed.
func (g *Group) Done() <-chan struct{} {
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.done
}

// Wait blocks until no subtasks are running, then returns the group result.
//
// The group result is set by finishing subtasks (see the documentation for
// OnExit modes) as well as by Exit calls.
func (g *Group) Wait() error {
	<-g.Done()

	return g.err
}

// Complete first waits for either the given context to close or the group to
// exit on its own, then for the group's remaining subtasks to finish.
//
// Returns the group result. If the group result is nil, returns the error from
// the given context so as to not confuse parallel.Fail if the group is empty.
//
// This is a convenience method useful when attaching a subgroup:
//
//	spawn("subgroup", parallel.Fail, subgroup.Complete)
//
// ...or:
//
//	group.Spawn("subgroup", parallel.Fail, subgroup.Complete)
func (g *Group) Complete(ctx context.Context) error {
	select {
	case <-ctx.Done():
	case <-g.ctx.Done():
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return ctx.Err()
}
