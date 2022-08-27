package parallel_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"fennel/lib/utils/parallel"

	"github.com/stretchr/testify/assert"
)

func TestWorkerPool(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	maxWorkers := runtime.GOMAXPROCS(0)
	workerPool := parallel.NewWorkerPool[int, int]("mypool", maxWorkers)
	start := time.Now()
	results, err := workerPool.Process(context.Background(), inputs, squareSlice, 64)
	elapsed := time.Since(start)
	assert.NoError(t, err)
	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	assert.Equal(t, expected, results)
	assert.LessOrEqual(t, elapsed, time.Duration(len(inputs)/maxWorkers+2)*time.Second)
}

func TestWorkerPoolWithError(t *testing.T) {
	var inputs []int
	for i := 0; i < 1000; i++ {
		inputs = append(inputs, i)
	}
	maxWorkers := runtime.GOMAXPROCS(0)
	workerPool := parallel.NewWorkerPool[int, int]("mypool", maxWorkers)
	f := func(x []int, y []int) error {
		for i := 0; i < len(x); i++ {
			if x[i]%4 == 0 {
				return errors.New("error")
			} else {
				y[i] = x[i] * x[i]
			}
		}
		return nil
	}
	_, err := workerPool.Process(context.Background(), inputs, f, 64)
	assert.Error(t, err)
}

func TestWorkerPoolWithSmallInput(t *testing.T) {
	var inputs []int
	for i := 1; i <= 10; i++ {
		inputs = append(inputs, i)
	}
	workerPool := parallel.NewWorkerPool[int, int]("mypool", 100)
	results, err := workerPool.Process(context.Background(), inputs, squareSlice, 64)
	assert.NoError(t, err)
	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	assert.Equal(t, expected, results)
}

func TestPoolEarlyExit(t *testing.T) {
	var inputs []int
	for i := 1; i <= 64*25; i++ {
		inputs = append(inputs, i)
	}
	workerPool := parallel.NewWorkerPool[int, int]("mypool", 1)
	e := errors.New("primerr")
	f := func(x []int, y []int) error {
		for i := 0; i < len(x); i++ {
			if x[i] == 47 {
				return e
			}
			y[i] = x[i] * x[i]
		}
		return nil
	}
	results, err := workerPool.Process(context.Background(), inputs, f, 64)
	assert.ErrorIs(t, err, e)
	zeroes := 0
	for _, x := range results {
		if x == 0 {
			zeroes++
		}
	}
	assert.Greater(t, zeroes, 1)
}

func TestPoolCancellation(t *testing.T) {
	var inputs []int
	for i := 1; i <= 64*25; i++ {
		inputs = append(inputs, i)
	}
	workerPool := parallel.NewWorkerPool[int, int]("mypool", 10)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	cancel()
	_, err := workerPool.Process(ctx, inputs, squareSlice, 64)
	assert.ErrorIs(t, err, context.Canceled)
}
