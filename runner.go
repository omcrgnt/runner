package runner

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"golang.org/x/sync/errgroup"
)

type runner struct {
	resources []any
}

func New(pool Pool) *runner {
	return &runner{resources: collect(pool)}
}

func collect(pool Pool) []any {
	var resources []any
	pool.Walk(func(_ reflect.Type, res any) bool {
		resources = append(resources, res)
		return true
	})
	return resources
}

func (r *runner) Run(rctx context.Context) error {
	group, ctx := errgroup.WithContext(rctx)

	for _, res := range r.resources {
		if s, ok := res.(Starter); ok {
			starter := s
			group.Go(func() error {
				if err := starter.Start(ctx); err != nil {
					return fmt.Errorf("starter %T failed: %w", starter, err)
				}
				return nil
			})
		}
	}

	return group.Wait()
}

func (r *runner) Stop(ctx context.Context) error {
	var errs []error

	for i := len(r.resources) - 1; i >= 0; i-- {
		res := r.resources[i]
		if sc, ok := res.(StartCloser); ok {
			if err := sc.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("active resource %T: %w", res, err))
			}
		}
	}

	for i := len(r.resources) - 1; i >= 0; i-- {
		res := r.resources[i]

		closer, isCloser := res.(Closer)
		_, isActive := res.(StartCloser)

		if isCloser && !isActive {
			if err := closer.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("passive resource %T: %w", res, err))
			}
		}
	}
	return errors.Join(errs...)
}
