package arena

import (
	"sync"
)

/*
	Arena is a memory allocator that can be used to allocate slices of arbitrary types.
	Unlike the standard library's slice allocator, Arena reserves a configurable amount of
	memory which isn't garbage collected until the arena is destroyed. This is useful for
	allocating temporary slices which are only used for a short time and are then created
	elsewhere -- in that sense, it acts like a buffer pool and saves on allocations + GC
	overhead.

	It operates by maintaining many page lists consisting of buffers of a certain size. When
	allocating a buffer, the arena will first check if there is a free page in the appropriate
	page list. If there is, it will allocate a buffer from that page. If there isn't, it will
	allocate a new page.

	Note: Free can also be called on a buffer that was not allocated from the arena.

	Arena is thread-safe and is currently implemented using an arena level lock. This isn't
	super efficient, but it's simple, and it works. Someday we may want to implement either
	a lock free version or at least have finer grained locking.


*/
type Arena[T any] struct {
	maxalloc  int
	capacity  int
	cursz     int
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
	if l := len([][]T(pl.pages)) - 1; l >= 0 {
		ret = pl.pages[l]
		pl.pages = pl.pages[:l]
		a.cursz -= cap(ret)
	}

	if cap(ret) < cap_ {
		// page miss, or the found page is too small
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
	// cleanup the slice by putting zero for all elements upto its cap
	var zero T
	b = b[:cap(b)]
	b[0] = zero
	for i := 1; i < len(b); i *= 2 {
		copy(b[i:], b[:i])
	}
	a.lock.Lock()
	defer a.lock.Unlock()
	if a.cursz+cap(b) > a.capacity {
		return
	}
	idx := idx(uint32(cap(b)))
	pl := a.pagelists[idx]
	pl.pages = append([][]T(pl.pages), b)
	a.cursz += cap(b)
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
