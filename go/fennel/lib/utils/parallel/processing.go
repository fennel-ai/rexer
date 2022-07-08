package parallel

import (
	"context"
	"github.com/Unleash/unleash-client-go/v3"
	"golang.org/x/sync/errgroup"
	"runtime"
	"strconv"
)

type item[T any] struct {
	input T
	index int
}

func Process[S, T any](ctx context.Context, inputs []S, f func(S) (T, error)) ([]T, error) {
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
	ret := make([]T, len(inputs))
	variant := unleash.GetVariant("parallel_processes")
	nWorkers := runtime.GOMAXPROCS(0)
	if variant.Enabled {
		percentage, err := strconv.Atoi(variant.Payload.Value)
		if err == nil {
			nWorkers = percentage * nWorkers / 100
			if nWorkers == 0 {
				nWorkers = 1
			}
		}
	}

	for i := 0; i < nWorkers; i++ {
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
