package gravel

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// This is not really a test - just a small utility to estimate the
// index overhead and the L1 index miss rate
func TestOverhead(t *testing.T) {
	rand.Seed(time.Now().Unix())
	for k := 0; k < 10; k++ {
		N := uint64(rand.Intn(10_000_000))
		PerBucket := uint64(9)
		Buckets := uint64(N/PerBucket) + 1
		data := make(map[uint64]int)
		for i := 0; i < int(N); i++ {
			num := rand.Uint64()
			slot := num % Buckets
			data[slot] += 1
		}
		l1miss := 0
		for _, v := range data {
			delta := v - 11
			if delta > 0 {
				l1miss += v
			}
		}
		Overhead := float64(32*Buckets) / float64(N)
		missrate := 100 * float64(l1miss) / float64(N)
		load := float64(N) / float64(PerBucket*Buckets)
		fmt.Printf("N: %d, number of bucket is: %d, load: %f, overhead: %f, miss: %f\n", N, Buckets, load, Overhead, missrate)
	}
}
