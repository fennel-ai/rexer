package parallel_test

import (
	"context"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"fennel/lib/utils/parallel"

	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func square(x int) (int, error) {
	return x * x, nil
}

func squareSleep(x int) (int, error) {
	time.Sleep(1 * time.Second)
	return x * x, nil
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

func countPrimes(limit int) (int, error) {
	primes := make([]int, limit)
	count := 0
	isPrimeDivisible := func(candidate int) bool {
		for j := 0; j < count; j++ {
			if math.Sqrt(float64(candidate)) < float64(primes[j]) {
				return false
			}
			isDivisibe := isDivisible(candidate, primes[j])
			if isDivisibe {
				return true
			}
		}
		return false
	}
	for candidate := 2; ; {
		if count < limit {
			if !isPrimeDivisible(candidate) {
				primes[count] = candidate
				count++
			}
			candidate++
		} else {
			break
		}
	}
	return primes[limit-1], nil
}

func isDivisible(candidate, by int) bool {
	return candidate%by == 0
}

func cpuIntensiveWork(sz int) {
	arr := make([]int, sz)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Int()
	}
	// sort the array
	sort.Ints(arr)
	// shuffle the array
	for i := len(arr) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		arr[i], arr[j] = arr[j], arr[i]
	}

}

var workerPool = parallel.NewWorkerPool[int, int](runtime.GOMAXPROCS(0))

func featureThread(mode string) {
	// Does some cpu intensive work
	// 1000 sized array takes 120 microseconds
	cpuIntensiveWork(rand.Intn(5000))
	arr := make([]int, 1000)
	for i := 0; i < len(arr); i++ {
		arr[i] = rand.Intn(1000) + 1
	}
	// Does some io work
	sleepTime := time.Duration(rand.Intn(20)) * time.Millisecond
	time.Sleep(sleepTime)
	// Does a lot more cpu intensive work
	switch mode {
	case "iterative":
		// Total takes around 12ms
		for i := 0; i < len(arr); i++ {
			// Each one takes around 120 microseconds
			countPrimes(arr[i])
		}
	case "parallel":
		parallel.Process(context.Background(), runtime.GOMAXPROCS(0), arr, countPrimes)
	case "pool":
		workerPool.Process(context.Background(), arr, countPrimes)
	}
}

// go test -tags dynamic -bench Benchmark_Parallelization -v fennel/lib/utils/parallel -run ^$
// iterative : 360ms
// parallel : 301ms
// worker pool : 424ms
func Benchmark_Parallelization(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 100; j++ {
			wg.Add(1)
			go func() {
				// 1 call takes ~25ms
				featureThread("pool")
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
