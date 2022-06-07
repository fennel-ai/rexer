package arena

import (
	"fmt"
	"sync"
	"time"
	"unsafe"

	"github.com/detailyang/fastrand-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	// Drop one free call in every 32 calls. This needs to be a power of 2
	dropRate = 32
)

/*
	Arena is a memory allocator that can be used to allocate slices of arbitrary types.
	Unlike the standard library's slice allocator, slices freed by Arena aren't available
	to GC right away - instead, they are (often) kept for reuse by someone else.
	This is useful for allocating temporary slices which are only used for a short time
	and are then created elsewhere -- in that sense, it acts like a buffer pool and saves
	on allocations + GC overhead.

	It has a few other similarities with sync.Pool:
	- It only allocates memory on-demand, and not when arena is initialized.
    - Free can also be called on a buffer that was not allocated from the arena.
	- As memory is freed, arena's memory footprint goes down (unless ofcourse no one else
	  is putting data back in) and can go down to zero. In other words, it doesn't OWN the
      memory forever
	- It randomly drops some free requests instead of putting them back in arena. This keeps
	  the pool dynamic and also further ensures that this arena doesn't unnecessarily hog memory

	It operates by maintaining many page lists consisting of buffers of a certain size. When
	allocating a buffer, the arena will first check if there is a free page in the appropriate
	page list. If there is, it will allocate a buffer from that page. If there isn't, it will
	allocate a new page.

	Arena is thread-safe and is currently implemented using arena level locks. It is simple and
	reasonably fast for what we need. But someday, we may want to implement a lock free version
	or at least have fine-grained locks.

*/
var stats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arena_stats",
	Help: "Stats about performance of arena",
}, []string{"metric", "name"})

type Arena[T any] struct {
	maxalloc  int
	capacity  int
	cursz     int
	name      string
	hits      int64
	misses    int64
	frees     int64
	drops     int64
	rejects   int64
	pagelists []*pageList[T]
	lock      sync.Mutex
}

// New creates an Arena where any single allocation can have max cap of `maxCap` and
// total cap of all items stored in arena can be upto `totalCap`
func New[T any](maxCap, totalCap int) *Arena[T] {
	ret := &Arena[T]{
		capacity: totalCap,
		maxalloc: maxCap,
	}
	maxpower := idx(uint32(maxCap))
	for i := 0; i <= int(maxpower); i++ {
		pl := pageList[T]{}
		ret.pagelists = append([]*pageList[T](ret.pagelists), &pl)
	}
	// start a goroutine to periodically report stats about arena
	go ret.report()
	return ret
}

type pageList[T any] struct {
	pages [][]T
}

// Alloc a slice of type T of length len_ and cap as cap_
func (a *Arena[T]) Alloc(len_, cap_ int) []T {
	if len_ > cap_ {
		cap_ = len_
	}
	if cap_ <= 0 {
		return nil
	}
	if cap_ >= a.maxalloc {
		return make([]T, len_, cap_)
	}

	idx := idx(uint32(cap_))
	pl := a.pagelists[idx]
	var ret []T

	a.lock.Lock()
	defer a.lock.Unlock()
	a.hits += 1
	if l := len([][]T(pl.pages)) - 1; l >= 0 {
		ret = pl.pages[l]
		pl.pages = pl.pages[:l]
		a.cursz -= cap(ret)
	}

	if cap(ret) < cap_ {
		// page miss, or the found page is too small
		a.misses += 1
		ret = make([]T, cap_)
	}

	return ret[:len_]
}

// Free returns a slice to the Arena allocator. It need not have
// been allocated via SliceAlloc().
func (a *Arena[T]) Free(b []T) {
	if cap(b) == 0 || cap(b) >= a.maxalloc {
		return // ignore out-of-range slices
	}
	drop := false
	if (fastrand.FastRand() & (dropRate - 1)) == 0 {
		drop = true // we will drop this but first need to maintain state counters
	}
	if !drop {
		// cleanup the slice by putting zero for all elements upto its cap
		var zero T
		b = b[:cap(b)]
		b[0] = zero
		for i := 1; i < len(b); i *= 2 {
			copy(b[i:], b[:i])
		}
	}
	a.lock.Lock()
	defer a.lock.Unlock()
	a.frees += 1
	if a.cursz+cap(b) > a.capacity {
		a.rejects += 1
		return
	}
	// Randomly drop few requests to keep the pool dynamic. This has two benefits -
	// 	1. Pool drains over time if traffic shifts to require a lower amount of space
	// 	2. Few large capacity items can hog the pool capacity and hurt "hit rate" for
	// 	   others. By constantly recycling some items, the "distribution" of arena
	// 	   should adjust with traffic needs
	if drop {
		a.drops += 1
		return
	}
	idx := idx(uint32(cap(b)))
	pl := a.pagelists[idx]
	pl.pages = append([][]T(pl.pages), b)
	a.cursz += cap(b)
}

// report publishes stats about this arena every 1 min
func (a *Arena[T]) report() {
	var zero T
	sz := int(unsafe.Sizeof(zero))
	name := fmt.Sprintf("%T_%d_%d", zero, a.maxalloc, a.capacity)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for ; true; <-ticker.C {
		func() {
			a.lock.Lock()
			defer a.lock.Unlock()
			stats.WithLabelValues("hits", name).Set(float64(a.hits))
			stats.WithLabelValues("misses", name).Set(float64(a.misses))
			stats.WithLabelValues("frees", name).Set(float64(a.frees))
			stats.WithLabelValues("drops", name).Set(float64(a.drops))
			stats.WithLabelValues("rejects", name).Set(float64(a.rejects))
			stats.WithLabelValues("size_bytes", name).Set(float64(sz * a.cursz))
		}()
	}
}

// bit like lg2(n/8)
// i.e. for n <=8, returns 0. for n in [9, 16), returns 1, for n in [16, 31), returns 2, etc.
func idx(n uint32) uint32 {
	var r uint32
	for n > 8 {
		n >>= 1
		r++
	}
	return r
}
