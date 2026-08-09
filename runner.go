package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

// Runner starts [Starter] and stops [Closer] resources injected via sdi after [Deps].
type Runner struct {
	starters      []Starter
	closers       []Closer
	started       []bool
	startedMu     sync.Mutex
	stopLifecycle context.CancelFunc
}

func (r *Runner) Deps() []any {
	return []any{
		([]Starter)(nil),
		([]Closer)(nil),
	}
}

func (r *Runner) Inject(args []any) {
	for _, arg := range args {
		switch v := arg.(type) {
		case []Starter:
			r.starters = v
		case []Closer:
			r.closers = v
		}
	}
}

func (r *Runner) Run(ctx context.Context) error {
	lifecycle, stop := context.WithCancel(ctx)
	r.stopLifecycle = stop

	r.startedMu.Lock()
	r.started = make([]bool, len(r.starters))
	r.startedMu.Unlock()

	var g errgroup.Group

	for i, s := range r.starters {
		i, starter := i, s
		g.Go(func() error {
			if err := starter.Start(lifecycle); err != nil {
				stop()
				return fmt.Errorf("starter %T failed: %w", starter, err)
			}
			r.setStarted(i, true)
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		r.releaseLifecycle()
		return err
	}
	return nil
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
