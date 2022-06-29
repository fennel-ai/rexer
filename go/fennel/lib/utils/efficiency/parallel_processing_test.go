package efficiency

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
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
	results, err := ProcessInParallel(context.Background(), inputs, square)
	assert.NoError(t, err)
	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	assert.Equal(t, expected, results)
	start := time.Now()
	results, err = ProcessInParallel(context.Background(), inputs, squareSleep)
	elapsed := time.Since(start)
	assert.NoError(t, err)
	assert.Equal(t, expected, results)
	// Iteratively it would have taken 10*2 = 20 seconds to process 10 inputs
	// Parallel processing would have taken 10 ( #input) /8 (workers ) *1 = 1.25 seconds to process 10 inputs
	assert.True(t, elapsed < 3*time.Second)
}
