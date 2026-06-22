package runner

import (
	"context"
	"errors"
	"fmt"

	"github.com/omcrgnt/res"
	"golang.org/x/sync/errgroup"
)

// Runner runs and stops [Starter]/[Closer] resources from a [res.Registry].
type Runner struct{}

func (r *Runner) NewResource() (any, error) {
	return &Runner{}, nil
}

type engine struct {
	resources []any
}

func newEngine(reg res.Registry) *engine {
	return &engine{resources: collect(reg)}
}

// New builds a runner engine from reg without registering a resource (tests).
func New(reg res.Registry) *engine {
	return newEngine(reg)
}

func collect(reg res.Registry) []any {
	var resources []any
	reg.WalkEntries(func(e res.Entry) bool {
		resources = append(resources, e.Value)
		return true
	})
	return resources
}

func (r *Runner) Run(ctx context.Context, reg res.Registry) error {
	return newEngine(reg).run(ctx)
}

func (r *Runner) Stop(ctx context.Context, reg res.Registry) error {
	return newEngine(reg).stop(ctx)
}

func (e *engine) run(rctx context.Context) error {
	group, ctx := errgroup.WithContext(rctx)

	for _, resource := range e.resources {
		if s, ok := resource.(Starter); ok {
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

func (e *engine) stop(ctx context.Context) error {
	var errs []error

	for i := len(e.resources) - 1; i >= 0; i-- {
		resource := e.resources[i]
		if sc, ok := resource.(StartCloser); ok {
			if err := sc.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("active resource %T: %w", resource, err))
			}
		}
	}

	for i := len(e.resources) - 1; i >= 0; i-- {
		resource := e.resources[i]

		closer, isCloser := resource.(Closer)
		_, isActive := resource.(StartCloser)

		if isCloser && !isActive {
			if err := closer.Close(ctx); err != nil {
				errs = append(errs, fmt.Errorf("passive resource %T: %w", resource, err))
			}
		}
	}
	return errors.Join(errs...)
}
