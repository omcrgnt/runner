package runner

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Runner starts [Starter] and stops [Closer] resources injected via sdi after [Deps].
type Runner struct {
	starters      []Starter
	closers       []Closer
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

	var g errgroup.Group

	for _, s := range r.starters {
		starter := s
		g.Go(func() error {
			if err := starter.Start(lifecycle); err != nil {
				stop()
				return fmt.Errorf("starter %T failed: %w", starter, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		r.releaseLifecycle()
		return err
	}
	return nil
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

	for i := len(r.closers) - 1; i >= 0; i-- {
		c := r.closers[i]
		if err := c.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closer %T: %w", c, err))
		}
	}

	return errors.Join(errs...)
}
