package parallel

import (
	"context"
	"golang.org/x/sync/errgroup"
	"runtime"
)

type item[T any] struct {
	input T
	index int
}

// Process runs the function f on each item in inputs in parallel. parallelism ranges from 1 to runtime.GOMAXPROCS(0)
func Process[S, T any](ctx context.Context, parallelism int, inputs []S, f func(S) (T, error)) ([]T, error) {
	g, ctx := errgroup.WithContext(ctx)
	itemCh := make(chan item[S])
	g.Go(func() error {
		defer close(itemCh)
		for i := range inputs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case itemCh <- item[S]{inputs[i], i}:
			}
		}
		return nil
	})
	if parallelism == 0 || parallelism > runtime.GOMAXPROCS(0) {
		parallelism = runtime.GOMAXPROCS(0)
	}
	ret := make([]T, len(inputs))
	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			for item := range itemCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					var err error
					if ret[item.index], err = f(item.input); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return ret, g.Wait()
}
