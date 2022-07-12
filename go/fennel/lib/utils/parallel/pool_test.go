package parallel_test

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"fennel/lib/utils/parallel"

	"github.com/stretchr/testify/assert"
)

func TestWorkerPool(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	maxWorkers := runtime.GOMAXPROCS(0)
	workerPool := parallel.NewWorkerPool[int, int](maxWorkers)
	start := time.Now()
	results, err := workerPool.Process(context.Background(), inputs, square)
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
	workerPool := parallel.NewWorkerPool[int, int](maxWorkers)
	f := func(x int) (int, error) {
		if x%4 == 0 {
			return 0, fmt.Errorf("error")
		}
		return square(x)
	}
	_, err := workerPool.Process(context.Background(), inputs, f)
	assert.Error(t, err)
}

func TestWorkerPoolWithSmallInput(t *testing.T) {
	var inputs []int
	for i := 1; i <= 10; i++ {
		inputs = append(inputs, i)
	}
	workerPool := parallel.NewWorkerPool[int, int](100)
	results, err := workerPool.Process(context.Background(), inputs, square)
	assert.NoError(t, err)
	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	assert.Equal(t, expected, results)
}
