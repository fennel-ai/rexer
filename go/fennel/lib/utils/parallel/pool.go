package parallel

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
)

var (
	jobQueueLen = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_pool_job_queue_len",
		Help: "Length of the worker pool job queue",
	}, []string{"name"})
	poolUtilization = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "worker_pool_utilization",
		Help: "Utilization in range [0.0, 1.0] of the worker pool",
	}, []string{"name"})
)

// WorkerPool is a pool of workers that can be used to run jobs.
// Jobs are submitted to the pool via the `Process` method.
type WorkerPool[I, O any] struct {
	// Name of the worker pool.
	name string
	// channel over which sub-tasks are dispatched to the workers.
	jobQueue chan job[I, O]
	// Number of workers in the pool.
	nWorkers int
	// Count of the number of workers that are currently running jobs.
	busyCount atomic.Int32
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

func NewWorkerPool[I, O any](name string, nWorkers int) *WorkerPool[I, O] {
	// We provide a lot of buffer in the job queue to avoid tail latency problems
	// that are caused by the overhead of goroutine scheduling delay.
	jobQueue := make(chan job[I, O], 100*nWorkers)
	wg := &sync.WaitGroup{}
	pool := WorkerPool[I, O]{
		name:     name,
		jobQueue: jobQueue,
		wg:       wg,
		nWorkers: nWorkers,
	}
	wg.Add(nWorkers)
	for i := 0; i < nWorkers; i++ {
		go pool.work()
	}
	// Report prometheus metrics for the pool.
	go pool.reportStats()

	return &pool
}

func (w *WorkerPool[I, O]) reportStats() {
	ticker := time.NewTicker(10 * time.Second)
	for range ticker.C {
		jobQueueLen.WithLabelValues(w.name).Set(float64(w.Len()))
		poolUtilization.WithLabelValues(w.name).Set(float64(w.Utilization()))
	}
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

func (w *WorkerPool[I, O]) Len() int {
	return len(w.jobQueue)
}

func (w *WorkerPool[I, O]) Utilization() float32 {
	return float32(w.busyCount.Load()) / float32(w.nWorkers)
}

func (w *WorkerPool[I, O]) Close() {
	close(w.jobQueue)
	w.wg.Wait()
}

func (w *WorkerPool[I, O]) work() {
	defer w.wg.Done()
	for job := range w.jobQueue {
		w.busyCount.Inc()
		var err error
		select {
		case <-job.ctx.Done():
			err = job.ctx.Err()
		default:
			err = job.f(job.inputs, job.outputs)
		}
		job.errChan <- err
		w.busyCount.Dec()
	}
}
