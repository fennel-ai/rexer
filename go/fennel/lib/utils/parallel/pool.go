package parallel

import (
	"context"
	"sync"
)

// WorkerPool is a pool of workers that can be used to run jobs.
// Jobs are submitted to the pool via the `Process` method.
type WorkerPool[I, O any] struct {
	// channel over which sub-tasks are dispatched to the workers.
	jobQueue chan job[I, O]
	// WaitGroup to wait for all goroutines to finish.
	wg *sync.WaitGroup
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

func NewWorkerPool[I, O any](nWorkers int) *WorkerPool[I, O] {
	// We provide a lot of buffer in the job queue to avoid tail latency problems
	// that are caused by the overhead of goroutine scheduling delay.
	jobQueue := make(chan job[I, O], 100*nWorkers)
	wg := &sync.WaitGroup{}
	pool := WorkerPool[I, O]{
		jobQueue: jobQueue,
		wg:       wg,
	}
	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go pool.work()
	}
	return &pool
}

func (w *WorkerPool[I, O]) Process(ctx context.Context, inputs []I, f func([]I, []O) error, batchSize int) ([]O, error) {
	ret := make([]O, len(inputs))
	numBatches := (len(inputs) + batchSize - 1) / batchSize
	errCh := make(chan error, numBatches)
	ctx, cancelFn := context.WithCancel(ctx)
	defer cancelFn()
	// Break the input into batches and submit them to the job queue.
	// After submission, we wait for the results to be available in the errCh
	// channel. If an error occurs, we cancel the context and wait for the
	// remaining jobs to return.
	for start := 0; start < len(inputs); start += batchSize {
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
	}
	var err error
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

func (w *WorkerPool[I, O]) work() {
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
}
