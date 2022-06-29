package efficiency

import (
	"context"
	"golang.org/x/sync/errgroup"
	"runtime"
)

type input struct {
	inp   interface{}
	index int
}

func ProcessInParallel[S, T any](ctx context.Context, inputs []S, f func(S) (T, error)) ([]T, error) {
	g, ctx := errgroup.WithContext(ctx)
	readers := make(chan input)
	var err error

	g.Go(func() error {
		defer close(readers)
		for i := range inputs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case readers <- input{inputs[i], i}:
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
					if ret[v.index], err = f(v.inp.(S)); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return ret, g.Wait()
}
