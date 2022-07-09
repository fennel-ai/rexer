package parallel

import (
	"context"
	"golang.org/x/sync/errgroup"
	"runtime"
)

type input[I any] struct {
	inp   I
	index int
}

type response[R any] struct {
	resp  R
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

// InitWorkerPool starts a pool of workers and returns a channel
// which needs to be passed while calling ProcessUsingWorkerPool.
func InitWorkerPool[I, R any](n int) chan interface{} {
	jobQueue := make(chan interface{})
	for i := 0; i < n; i++ {
		worker := NewWorker[input[I], response[R]](jobQueue)
		worker.Start()
	}
	return jobQueue
}

// ProcessUsingWorkerPool is similar to Process but uses a worker pool rather than spinning up individual workers determined
// by the parallelism. The only difference is that this function takes a jobQueue while Process takes a parallelism.
func ProcessUsingWorkerPool[I, R any](ctx context.Context, inputs []I, jobQueue chan interface{}, f func(I) (R, error)) ([]R, error) {
	ret := make([]R, len(inputs))
	retChan := make(chan response[R])
	errChan := make(chan error)
	wrappedF := func(i input[I]) (response[R], error) {
		r, err := f(i.inp)
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
			case <-ctx.Done():
				return ctx.Err()
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
