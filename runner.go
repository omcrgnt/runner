package runner

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"
)

// Runner starts [Starter] and stops [Closer] resources injected via sdi after [Deps].
type Runner struct {
	starters []Starter
	closers  []Closer
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
	group, ctx := errgroup.WithContext(ctx)

	for _, s := range r.starters {
		starter := s
		group.Go(func() error {
			if err := starter.Start(ctx); err != nil {
				return fmt.Errorf("starter %T failed: %w", starter, err)
			}
			return nil
		})
	}

	return group.Wait()
}

func (r *Runner) Stop(ctx context.Context) error {
	var errs []error

	for i := len(r.closers) - 1; i >= 0; i-- {
		c := r.closers[i]
		if err := c.Close(ctx); err != nil {
			errs = append(errs, fmt.Errorf("closer %T: %w", c, err))
		}
	}

	return errors.Join(errs...)
}
