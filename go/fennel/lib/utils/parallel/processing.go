package parallel

import (
	"context"
	"golang.org/x/sync/errgroup"
	"runtime"
)

type input[I any] struct {
	inp   interface{}
	index int
}

type response[R any] struct {
	resp  R
	index int
}

func Process[S, T any](ctx context.Context, parallelism int, inputs []S, f func(S) (T, error)) ([]T, error) {
	g, ctx := errgroup.WithContext(ctx)
	itemCh := make(chan input[S])
	g.Go(func() error {
		defer close(itemCh)
		for i := range inputs {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case itemCh <- input[S]{inputs[i], i}:
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
					inp := item.inp.(S)
					if ret[item.index], err = f(inp); err != nil {
						return err
					}
				}
			}
			return nil
		})
	}
	return ret, g.Wait()
}

func InitWorkerPool[I, R any](n int, jobQueue chan interface{}) {
	for i := 0; i < n; i++ {
		worker := NewWorker[input[I], response[R]](jobQueue)
		worker.Start()
	}
}

func ProcessUsingWorkerPool[I, R any](ctx context.Context, inputs []I, jobQueue chan interface{}, f func(I) (R, error)) ([]R, error) {
	ret := make([]R, len(inputs))
	retChan := make(chan response[R])
	errChan := make(chan error)
	wrappedF := func(i input[I]) (response[R], error) {
		inp := i.inp.(I)
		r, err := f(inp)
		if err != nil {
			return response[R]{}, err
		}
		return response[R]{r, i.index}, nil
	}
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for i := 0; i < len(inputs); i++ {
			select {
			case r := <-retChan:
				ret[r.index] = r.resp
			case err := <-errChan:
				return err
			}
		}
		close(retChan)
		close(errChan)
		return nil
	})
	for i := range inputs {
		jobQueue <- Job[input[I], response[R]]{input[I]{inputs[i], i}, wrappedF, retChan, errChan}
	}
	return ret, g.Wait()
}

// Job represents the job to be run. It accepts a function F that needs to be run on
// an input of type I and returns the response of type R in the return channel passed to it.
type Job[I, R any] struct {
	Input   I
	F       func(I) (R, error)
	RetChan chan R
	ErrChan chan error
}

// Worker represents the worker that executes the job
type Worker[I, R any] struct {
	JobQueue chan interface{}
}

func NewWorker[I, R any](jobQueue chan interface{}) Worker[I, R] {
	return Worker[I, R]{
		JobQueue: jobQueue,
	}
}

// Start method starts the run loop for the worker.
func (w Worker[I, R]) Start() {
	go func() {
		for {
			select {
			case j := <-w.JobQueue:
				job := j.(Job[I, R])
				// we have received a work request.
				resp, err := job.F(job.Input)
				if err != nil {
					job.ErrChan <- err
				}
				job.RetChan <- resp
			}
		}
	}()
}
