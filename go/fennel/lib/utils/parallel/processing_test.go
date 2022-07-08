package parallel_test

import (
	"context"
	"testing"
	"time"

	"fennel/lib/utils/parallel"

	"github.com/stretchr/testify/assert"
)

func square(x int) (int, error) {
	return x * x, nil
}

func squareSleep(x int) (int, error) {
	time.Sleep(1 * time.Second)
	return x * x, nil
}

func TestParallelProcessing(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	results, err := parallel.Process(context.Background(), 2, inputs, square)
	assert.NoError(t, err)
	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	assert.Equal(t, expected, results)
	start := time.Now()
	results, err = parallel.Process(context.Background(), 4, inputs, squareSleep)
	elapsed := time.Since(start)
	assert.NoError(t, err)
	assert.Equal(t, expected, results)
	// Iteratively it would have taken 10*1 = 10 seconds to process 10 inputs
	// Parallel processing would have taken 10 ( #input) /(num workers) * 1.
	// We add a 2 second buffer to account for scheduling delays and rounding.
	assert.LessOrEqual(t, elapsed, time.Duration(len(inputs)/4+2)*time.Second)
}
