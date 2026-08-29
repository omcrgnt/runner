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

// Runner starts [Starter] and stops [Closer] resources injected via sdi after [Deps].
type Runner struct {
	starters      []Starter
	closers       []Closer
	gate          gateOpener
	started       []bool
	startedMu     sync.Mutex
	stopLifecycle context.CancelFunc
}

func (r *Runner) Deps() []any {
	return []any{
		([]Starter)(nil),
		([]Closer)(nil),
		([]gateOpener)(nil),
	}
}

func (r *Runner) Inject(args []any) {
	for _, arg := range args {
		switch v := arg.(type) {
		case []Starter:
			r.starters = v
		case []Closer:
			r.closers = v
		case []gateOpener:
			// A gate is optional (fail-open when nothing wires one) — a
			// registry that never registers a Gate (e.g. a test building a
			// fresh unique.New() registry by hand) must still resolve.
			// sdi.Resolve only tolerates zero matches for a many-dep
			// ([]T stub), not a single-dep one.
			if len(v) > 0 {
				r.gate = v[0]
			}
		}
	}
}

func (r *Runner) Run(ctx context.Context) error {
	lifecycle, stop := context.WithCancel(ctx)
	r.stopLifecycle = stop

	r.startedMu.Lock()
	r.started = make([]bool, len(r.starters))
	r.startedMu.Unlock()

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
		return err
	}

	if r.gate != nil {
		r.gate.Open()
	}

	if err := r.startBatch(lifecycle, stop, lastIdx); err != nil {
		r.releaseLifecycle()
		return err
	}
	return nil
}

func (r *Runner) startBatch(lifecycle context.Context, stop context.CancelFunc, idx []int) error {
	var g errgroup.Group

	for _, i := range idx {
		i, starter := i, r.starters[i]
		g.Go(func() error {
			if err := starter.Start(lifecycle); err != nil {
				stop()
				return fmt.Errorf("starter %T failed: %w", starter, err)
			}
			r.setStarted(i, true)
			return nil
		})
	}

	return g.Wait()
}

func (r *Runner) setStarted(i int, v bool) {
	r.startedMu.Lock()
	defer r.startedMu.Unlock()
	if i >= 0 && i < len(r.started) {
		r.started[i] = v
	}
}

func (r *Runner) isStarted(i int) bool {
	r.startedMu.Lock()
	defer r.startedMu.Unlock()
	return i >= 0 && i < len(r.started) && r.started[i]
}

func (r *Runner) releaseLifecycle() {
	if r.stopLifecycle == nil {
		return
	}
	r.stopLifecycle()
	r.stopLifecycle = nil
}

func (r *Runner) Stop(ctx context.Context) error {
	r.releaseLifecycle()

	var errs []error

	for i, s := range r.starters {
		if !r.isStarted(i) {
			continue
		}
		c, ok := s.(Closer)
		if !ok {
			continue
		}
		if err := c.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closer %T: %w", c, err))
		}
	}

	for _, c := range r.closers {
		if r.isAlsoStarter(c) {
			continue
		}
		if err := c.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closer %T: %w", c, err))
		}
	}

	return errors.Join(errs...)
}

func (r *Runner) isAlsoStarter(c Closer) bool {
	for _, s := range r.starters {
		if any(s) == any(c) {
			return true
		}
	}
	return false
}
