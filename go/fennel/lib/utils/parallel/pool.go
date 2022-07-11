package parallel

import "context"

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
	// We use a batch size of 16 to dispatch jobs to the workers. This amortizes
	// the synchronization cost of sending a job to a worker, but does not make
	// the batch so large that we lose the benefit of parallelism.
	// A batch size of 16 performs reasonably well in our benchmark.
	const BATCH_SIZE = 16
	numBatches := (len(inputs) + BATCH_SIZE - 1) / BATCH_SIZE
	// Note: We don't close errCh because we early-return in case of an error,
	// but don't want worker go-routines to panic if they try to write on
	// a closed channel. This is also why we create a buffered channel, so that
	// worker go-routines can write to it even if there is no consumer.
	errCh := make(chan error, numBatches)
	for i := 0; i < len(inputs); {
		end := i + BATCH_SIZE
		if end > len(inputs) {
			end = len(inputs)
		}
		w.jobQueue <- job[I, O]{
			inputs:  inputs[i:end],
			outputs: ret[i:end],
			f:       f,
			errChan: errCh,
		}
		i = end
	}
	for i := 0; i < numBatches; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errCh:
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}

// job represents the job to be run. It accepts a function `f` that needs to be
// run on inputs of type I and writes outputs of type O to the given `outputs``
// slice. If an error is encountered, it is written to the `errChan` and the
// remaining slice is not processed.
type job[I, O any] struct {
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
			for i, input := range job.inputs {
				job.outputs[i], err = job.f(input)
				if err != nil {
					break
				}
			}
			job.errChan <- err
		}
	}()
}
