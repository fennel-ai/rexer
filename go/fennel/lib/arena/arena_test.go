package arena

import (
	"fennel/lib/value"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testArena_Any[T any](t *testing.T) {
	// pool that allocates upto 4KB for a total capacity of 1MB
	a := New[T](1<<12, 1<<20)
	sizes := []int{1, 0, 123, 129, 23, 20, 2400}
	for _, size := range sizes {
		var slice []T
		var len_ int
		if size == 0 {
			len_ = 0
		} else {
			len_ = rand.Intn(size)
		}
		slice = a.Alloc(len_, size)
		assert.Len(t, slice, len_)
		assert.GreaterOrEqual(t, cap(slice), size)
		// verify all elements upto cap are zero
		slice = slice[:cap(slice)]
		var zero T
		for j := 0; j < len(slice); j++ {
			assert.Equal(t, zero, slice[j])
		}
		a.Free(slice)
	}
}

func TestArena(t *testing.T) {
	testArena_Any[byte](t)
	testArena_Any[[]byte](t)
	testArena_Any[value.Value](t)
	testArena_Any[error](t)
}

func TestArenaConcurrent(t *testing.T) {
	a := New[byte](1<<12, 1<<20)
	sizes := []int{1, 0, 123, 129, 23, 20, 2400}
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, size := range sizes {
				var slice []byte
				slice = a.Alloc(size, 2*size)
				assert.Len(t, slice, size)
				assert.GreaterOrEqual(t, cap(slice), 2*size)
				// verify all elements upto cap are zero
				slice = slice[:cap(slice)]
				zero := byte(0)
				for j := 0; j < len(slice); j++ {
					assert.Equal(t, zero, slice[j])
				}
				a.Free(slice)
			}
		}()
	}
	wg.Wait()
}

var buf []byte

func Benchmark_Alloc_ByteSlice(b *testing.B) {
	a := New[byte](1<<12, 1<<20)
	for i := 0; i < b.N; i++ {
		c := a.Alloc(123, 256)
		a.Free(c)
	}
}

func Benchmark_Alloc_Heap(b *testing.B) {
	for i := 0; i < b.N; i++ {
		buf = make([]byte, 123, 256)
		_ = buf // this is to avoid compiler optimization where buf is created on stack
	}
}
