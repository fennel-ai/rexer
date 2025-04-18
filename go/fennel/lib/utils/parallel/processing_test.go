package parallel_test

import (
	"context"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"fennel/lib/utils/parallel"

	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestParallelProcessing(t *testing.T) {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	maxWorkers := runtime.GOMAXPROCS(0)
	results, err := parallel.Process(context.Background(), maxWorkers, inputs, square)
	assert.NoError(t, err)
	expected := []int{1, 4, 9, 16, 25, 36, 49, 64, 81, 100}
	assert.Equal(t, expected, results)
	start := time.Now()
	results, err = parallel.Process(context.Background(), maxWorkers, inputs, squareSleep)
	elapsed := time.Since(start)
	assert.NoError(t, err)
	assert.Equal(t, expected, results)
	// Iteratively it would have taken 10*1 = 10 seconds to process 10 inputs
	// Parallel processing would have taken 10 ( #input) /(num workers) * 1.
	// We add a 2 second buffer to account for scheduling delays and rounding.
	assert.LessOrEqual(t, elapsed, time.Duration(len(inputs)/maxWorkers+2)*time.Second)
}
