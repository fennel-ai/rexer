package parallel

import (
	"context"
	"sync"
)

// WorkerPool is a pool of workers that can be used to run jobs.
// Jobs are submitted to the pool via the `Process` method.
type WorkerPool[I, O any] struct {
	// channel over which sub-tasks are dispatched to the workers.
	jobQueue chan<- job[I, O]
	// WaitGroup to wait for all goroutines to finish.
	wg *sync.WaitGroup
}

func NewWorkerPool[I, O any](nWorkers int) *WorkerPool[I, O] {
	jobQueue := make(chan job[I, O])
	wg := &sync.WaitGroup{}
	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		worker := worker[I, O]{jobQueue, wg}
		worker.start()
	}
	return &WorkerPool[I, O]{jobQueue, wg}
}

func (w *WorkerPool[I, O]) Process(ctx context.Context, inputs []I, f func([]I, []O) error, batchSize int) ([]O, error) {
	ret := make([]O, len(inputs))
	numBatches := (len(inputs) + batchSize - 1) / batchSize
	errCh := make(chan error, numBatches)
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	// Run the job submission in a go-routine, so that we can start consuming
	// results/errors from errCh and cancel any outstanding jobs.
	go func() {
		start := 0
		for i := 0; i < numBatches; i++ {
			end := start + batchSize
			if end > len(inputs) {
				end = len(inputs)
			}
			w.jobQueue <- job[I, O]{
				ctx:     ctx,
				inputs:  inputs[start:end],
				outputs: ret[start:end],
				f:       f,
				errChan: errCh,
			}
			start = end
		}
	}()
	var err error
	// We wait for all the jobs to complete.
	for i := 0; i < numBatches; i++ {
		e := <-errCh
		if e != nil {
			cancelFn()
			// We want to capture and return the first error encountered.
			if err == nil {
				err = e
			}
		}
	}
	return ret, err
}

func (w *WorkerPool[I, O]) Close() {
	close(w.jobQueue)
	w.wg.Wait()
}

// job represents the job to be run. It accepts a function `f` that needs to be
// run on inputs of type I and writes outputs of type O to the given `outputs``
// slice. If an error is encountered, it is written to the `errChan` and the
// remaining slice is not processed.
type job[I, O any] struct {
	ctx     context.Context
	inputs  []I
	outputs []O
	f       func([]I, []O) error
	errChan chan<- error
}

// worker accepts a job from `jobQueue` and runs it.
type worker[I, O any] struct {
	jobQueue <-chan job[I, O]
	wg       *sync.WaitGroup
}

// start method starts the run loop for the worker.
func (w worker[I, O]) start() {
	go func() {
		defer w.wg.Done()
		for job := range w.jobQueue {
			var err error
			select {
			case <-job.ctx.Done():
				err = job.ctx.Err()
			default:
				err = job.f(job.inputs, job.outputs)
			}
			job.errChan <- err
		}
	}()
}
