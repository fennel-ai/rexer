package parallel

import (
	"context"
	"runtime"

	"golang.org/x/sync/errgroup"
)

type input[I any] struct {
	inp   I
	index int
}

// Process takes a list of inputs, the degree of parallelism ( capped by max cpu cores )
// and a function that processes each input.
func Process[I, R any](ctx context.Context, parallelism int, inputs []I, f func(I) (R, error)) ([]R, error) {
	g, ctx := errgroup.WithContext(ctx)
	itemCh := make(chan input[I])
	g.Go(func() error {
		defer close(itemCh)
		for i := range inputs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case itemCh <- input[I]{inputs[i], i}:
			}
		}
		return nil
	})
	if parallelism == 0 || parallelism > runtime.GOMAXPROCS(0) {
		parallelism = runtime.GOMAXPROCS(0)
	}
	ret := make([]R, len(inputs))
	for i := 0; i < parallelism; i++ {
		g.Go(func() error {
			for item := range itemCh {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					var err error
					if ret[item.index], err = f(item.inp); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return ret, g.Wait()
}
