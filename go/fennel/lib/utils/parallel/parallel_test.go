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

func square(x int) (int, error) {
	return x * x, nil
}

func squareSleep(x int) (int, error) {
	time.Sleep(1 * time.Second)
	return x * x, nil
}

func squareSlice(x []int, y []int) error {
	for i := 0; i < len(x); i++ {
		y[i] = x[i] * x[i]
	}
	return nil
}

func countPrimesSlice(limits []int, count []int) error {
	var err error
	for i := 0; i < len(limits); i++ {
		count[i], err = countPrimes(limits[i])
		if err != nil {
			return err
		}
	}
	return nil
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

var workerPool = parallel.NewWorkerPool[int, int]("mypool", runtime.GOMAXPROCS(0))

func featureThread(b *testing.B, mode string) {
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
			_, err := countPrimes(arr[i])
			assert.NoError(b, err)
		}
	case "parallel":
		_, err := parallel.Process(context.Background(), runtime.GOMAXPROCS(0), arr, countPrimes)
		assert.NoError(b, err)
	case "pool":
		_, err := workerPool.Process(context.Background(), arr, countPrimesSlice, 64)
		assert.NoError(b, err)
	}
}

// go test -tags dynamic -bench Benchmark_Parallelization -v fennel/lib/utils/parallel -run ^$
// iterative : 332ms
// parallel : 292ms
// worker pool : 286ms
func Benchmark_Parallelization(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for j := 0; j < 100; j++ {
			wg.Add(1)
			go func() {
				// 1 call takes ~25ms
				featureThread(b, "pool")
				wg.Done()
			}()
		}
		wg.Wait()
	}
}
