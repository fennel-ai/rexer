package parallel

import (
	"context"
	"runtime"

	"golang.org/x/sync/errgroup"
)

type input[T any] struct {
	inp   T
	index int
}

func Process[S, T any](ctx context.Context, inputs []S, f func(S) (T, error)) ([]T, error) {
	g, ctx := errgroup.WithContext(ctx)
	readers := make(chan input[S])
	var err error

	g.Go(func() error {
		defer close(readers)
		for i := range inputs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case readers <- input[S]{inputs[i], i}:
			}
		}
		return nil
	})

	ret := make([]T, len(inputs))
	nWorkers := runtime.GOMAXPROCS(0)
	for i := 0; i < nWorkers; i++ {
		g.Go(func() error {
			for v := range readers {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					if ret[v.index], err = f(v.inp); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return ret, g.Wait()
}
