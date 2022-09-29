package arena

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type A struct {
	bytes []byte
}

type B struct {
	x     int
	y     bool
	a     A
	slice []A
}

func TestRegion(t *testing.T) {
	// spin up lots of goroutines each of which do a lot of allocations from
	// regions in complex self-referential ways and verify data is always correct
	for req := 0; req < 10; req++ {
		r := NewRegion()
		defer r.Free()
		wg := sync.WaitGroup{}
		for w := 0; w < 20; w++ {
			go func() {
				wg.Add(1)
				defer wg.Done()
				var as []A
				var bs []B
				for i := 0; i < 100; i++ {
					a := Alloc[A](r)
					a.bytes = []byte(fmt.Sprintf("i:%d", i))
					as = append(as, *a)
					b := Alloc[B](r)
					b.x = i
					b.y = i%2 == 0
					b.a = *a
					b.slice = as
					bs = append(bs, *b)
				}
				var sofar []A
				for i := 0; i < 100; i++ {
					a, b := as[i], bs[i]
					assert.Equal(t, []byte(fmt.Sprintf("i:%d", i)), a.bytes)
					assert.Equal(t, a, b.a)
					assert.Equal(t, i, b.x)
					assert.Equal(t, i%2 == 0, b.y)
					sofar = append(sofar, a)
					assert.Equal(t, sofar, b.slice)
				}
			}()
		}
		wg.Wait()
	}
}
