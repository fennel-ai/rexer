package parallel

import (
	"context"
)

const (
	// We use a default batch size of 64 to dispatch jobs to the workers. This
	// amortizes the synchronization cost of sending a job to a worker, but does
	// not make the batch so large that we lose the benefit of parallelism.
	// A batch size of 64 performs reasonably well in our benchmark.
	batchSize = 64
)

// WorkerPool is a pool of workers that can be used to run jobs.
// Jobs are submitted to the pool via the `Process` method.
type WorkerPool[I, O any] struct {
	// channel over which sub-tasks are dispatched to the workers.
	jobQueue chan<- job[I, O]
}

func NewWorkerPool[I, O any](nWorkers int) WorkerPool[I, O] {
	jobQueue := make(chan job[I, O])
	for i := 0; i < nWorkers; i++ {
		worker := newWorker(jobQueue)
		worker.start()
	}
	return WorkerPool[I, O]{jobQueue: jobQueue}
}

func (w *WorkerPool[I, O]) Process(ctx context.Context, inputs []I, f func(I) (O, error)) ([]O, error) {
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

// job represents the job to be run. It accepts a function `f` that needs to be
// run on inputs of type I and writes outputs of type O to the given `outputs``
// slice. If an error is encountered, it is written to the `errChan` and the
// remaining slice is not processed.
type job[I, O any] struct {
	ctx     context.Context
	inputs  []I
	outputs []O
	f       func(I) (O, error)
	errChan chan<- error
}

// worker accepts a job from `jobQueue` and runs it.
type worker[I, O any] struct {
	jobQueue <-chan job[I, O]
}

func newWorker[I, O any](jobQueue chan job[I, O]) worker[I, O] {
	return worker[I, O]{
		jobQueue: jobQueue,
	}
}

// start method starts the run loop for the worker.
func (w worker[I, O]) start() {
	go func() {
		for job := range w.jobQueue {
			var err error
			select {
			case <-job.ctx.Done():
				err = job.ctx.Err()
			default:
				for i, input := range job.inputs {
					job.outputs[i], err = job.f(input)
					if err != nil {
						break
					}
				}
			}
			job.errChan <- err
		}
	}()
}
