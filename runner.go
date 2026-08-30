package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

// gateOpener is Runner's write-side view of [Gate] — a local, narrow
// interface (rather than depending on *Gate directly) so a fake gate can be
// injected in tests without constructing a real Gate's unexported channel.
type gateOpener interface {
	Open()
}

// Runner runs the [StandBy] phase, then starts [Starter] and stops
// [Closer]/Starter/StandBy cleanups, all injected via sdi after [Deps].
type Runner struct {
	starters []Starter
	standBys []StandBy
	closers  []Closer
	gate     gateOpener

	mu              sync.Mutex
	started         []bool
	startCleanups   []func(context.Context) error
	standByCleanups []func(context.Context) error

	stopLifecycle context.CancelFunc
}

func (r *Runner) Deps() []any {
	return []any{
		([]Starter)(nil),
		([]StandBy)(nil),
		([]Closer)(nil),
		(*gateOpener)(nil),
	}
}

func (r *Runner) Inject(args []any) {
	for _, arg := range args {
		switch v := arg.(type) {
		case []Starter:
			r.starters = v
		case []StandBy:
			r.standBys = v
		case []Closer:
			r.closers = v
		case gateOpener:
			r.gate = v
		}
	}
}

func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	r.standByCleanups = make([]func(context.Context) error, len(r.standBys))
	r.mu.Unlock()

	if err := r.runStandBy(); err != nil {
		return err
	}

	lifecycle, stop := context.WithCancel(ctx)
	r.stopLifecycle = stop

	r.mu.Lock()
	r.started = make([]bool, len(r.starters))
	r.startCleanups = make([]func(context.Context) error, len(r.starters))
	r.mu.Unlock()

	var normalIdx, lastIdx []int
	for i, s := range r.starters {
		if _, ok := s.(interface{ LastStart() }); ok {
			lastIdx = append(lastIdx, i)
		} else {
			normalIdx = append(normalIdx, i)
		}
	}

	if err := r.startBatch(lifecycle, stop, normalIdx); err != nil {
		r.releaseLifecycle()
		return errors.Join(err, r.rollbackStarted(context.Background()), r.rollbackStandByUpTo(len(r.standBys)))
	}

	if err := r.startBatch(lifecycle, stop, lastIdx); err != nil {
		r.releaseLifecycle()
		return errors.Join(err, r.rollbackStarted(context.Background()), r.rollbackStandByUpTo(len(r.standBys)))
	}

	// Open only once both waves have fully succeeded: Ready() must never
	// report true while Run is about to return an error, and neither wave's
	// Start reads the gate synchronously (only per-request middleware does,
	// lazily) — so there is nothing waiting on it to open any earlier.
	if r.gate != nil {
		r.gate.Open()
	}
	return nil
}

// runStandBy runs every StandBy sequentially, in registration order, before
// the Start phase. On failure it unwinds whatever earlier StandBy calls
// already set up, in reverse order, and joins that into the returned error.
func (r *Runner) runStandBy() error {
	for i, sb := range r.standBys {
		cleanup, err := sb.StandBy()
		if err != nil {
			return errors.Join(fmt.Errorf("standby %T failed: %w", sb, err), r.rollbackStandByUpTo(i))
		}
		r.mu.Lock()
		r.standByCleanups[i] = cleanup
		r.mu.Unlock()
	}
	return nil
}

func (r *Runner) startBatch(lifecycle context.Context, stop context.CancelFunc, idx []int) error {
	var g errgroup.Group

	for _, i := range idx {
		i, starter := i, r.starters[i]
		g.Go(func() error {
			cleanup, err := starter.Start(lifecycle)
			if err != nil {
				stop()
				return fmt.Errorf("starter %T failed: %w", starter, err)
			}
			r.mu.Lock()
			r.startCleanups[i] = cleanup
			r.mu.Unlock()
			r.setStarted(i, true)
			return nil
		})
	}

	return g.Wait()
}

// rollbackStarted closes the cleanup of every Starter that had already
// reported success by the time a later Starter failed — reverse
// registration order, concurrently, since the starters themselves were
// started concurrently too. Consumed cleanups are cleared so a later Stop
// does not invoke them again.
//
// ctx is handed to each cleanup as-is: callers pass context.Background()
// for Run's own internal unwind (the lifecycle ctx is already being torn
// down at that point, so it's not a usable deadline source), and Stop
// passes its caller-supplied ctx through unchanged — same as it already
// does for StandBy cleanups and pure Closers below.
func (r *Runner) rollbackStarted(ctx context.Context) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for i := len(r.starters) - 1; i >= 0; i-- {
		if !r.isStarted(i) {
			continue
		}
		cleanup := r.takeStartCleanup(i)
		if cleanup == nil {
			continue
		}
		starter := r.starters[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := cleanup(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("start cleanup %T: %w", starter, err))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	return errors.Join(errs...)
}

// rollbackStandByUpTo closes the cleanups of standBys[0:n], reverse order,
// sequentially (StandBy itself is sequential, not concurrent). Consumed
// cleanups are cleared so a later Stop does not invoke them again.
func (r *Runner) rollbackStandByUpTo(n int) error {
	var errs []error

	for i := n - 1; i >= 0; i-- {
		cleanup := r.takeStandByCleanup(i)
		if cleanup == nil {
			continue
		}
		if err := cleanup(context.Background()); err != nil {
			errs = append(errs, fmt.Errorf("standby cleanup %T: %w", r.standBys[i], err))
		}
	}

	return errors.Join(errs...)
}

func (r *Runner) takeStartCleanup(i int) func(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < 0 || i >= len(r.startCleanups) {
		return nil
	}
	cleanup := r.startCleanups[i]
	r.startCleanups[i] = nil
	return cleanup
}

func (r *Runner) takeStandByCleanup(i int) func(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i < 0 || i >= len(r.standByCleanups) {
		return nil
	}
	cleanup := r.standByCleanups[i]
	r.standByCleanups[i] = nil
	return cleanup
}

func (r *Runner) setStarted(i int, v bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= 0 && i < len(r.started) {
		r.started[i] = v
	}
}

func (r *Runner) isStarted(i int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return i >= 0 && i < len(r.started) && r.started[i]
}

func (r *Runner) releaseLifecycle() {
	if r.stopLifecycle == nil {
		return
	}
	r.stopLifecycle()
	r.stopLifecycle = nil
}

// Stop releases the lifecycle cancel func, then closes, in this order:
// every started Starter's cleanup (reverse registration order,
// concurrently), then every StandBy's cleanup together with every pure
// Closer (each reverse registration order; the two lists' relative order
// does not matter — StandBy/Closer resources were all ready before the
// Start phase began).
func (r *Runner) Stop(ctx context.Context) error {
	r.releaseLifecycle()

	startErr := r.rollbackStarted(ctx)

	var errs []error
	if startErr != nil {
		errs = append(errs, startErr)
	}

	for i := len(r.standBys) - 1; i >= 0; i-- {
		cleanup := r.takeStandByCleanup(i)
		if cleanup == nil {
			continue
		}
		if err := cleanup(ctx); err != nil {
			errs = append(errs, fmt.Errorf("standby cleanup %T: %w", r.standBys[i], err))
		}
	}

	for i := len(r.closers) - 1; i >= 0; i-- {
		c := r.closers[i]
		if err := c.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closer %T: %w", c, err))
		}
	}

	return errors.Join(errs...)
}
