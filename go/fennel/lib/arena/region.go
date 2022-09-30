package arena

import (
	"fennel/lib/utils/slice"
	"sync"
	"unsafe"

	"github.com/detailyang/fastrand-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

/*
		Region is an abstraction over memory allocator to allocate memory
		for a request on demand and then free it all up together when the
		request is over.

		Internally, it maintains a list of pages of 1MB size. Whenever new
		memory is needed, it scans its list of pages and find a page that
		has enough space left. If no such page is found, it allocates a new
		1MB page. It's concurrency safe and uses a lock to guard the access.

		It may not a good fit for the following cases:

		1. When the size of individual allocations are bigger than 1MB. All
	       such requests are directly given to the runtime and hence gain
	       nothing from the region.

		2. When the total size of a request's allocation is substantially
	       smaller than 1MB and there are lots of such requests. In these
	       cases, each region will still own at least 1MB of RAM, which
	       leads to a lot of memory being blocked but not used. This can be
		   somewhat fixed by sharing a region across requests, but that is
	       some work on the user side.

		3. When a single request is spawning LOTs of goroutines each of which
		   are requesting memory concurrently. In such cases, contention of
		   the lock could become a bottleneck.
*/

var regionStats = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "arena_region_stats",
	Help: "Aggregate stats about performance of memory regions",
}, []string{"metric"})

var regionSummaryStats = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "arena_region_summary",
	Help: "Stats about performance of each region",
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"metric"})

const (
	pagesize = 1 << 20 // 1 MB page
)

type Region struct {
	pages [][]byte // list of 1MB pages that this region owns
	used  []int    // how much memory is used in each page
	lock  sync.RWMutex
}

var pagepool = sync.Pool{New: func() any {
	return make([]byte, pagesize)
}}

const samplerate = 1024

func shouldReport() bool {
	return fastrand.FastRand()&(samplerate-1) == 0
}

func NewRegion() *Region {
	if shouldReport() {
		regionStats.WithLabelValues("new").Add(float64(samplerate))
	}
	return &Region{
		pages: nil,
		used:  nil,
		lock:  sync.RWMutex{},
	}
}

// alloc allocates a byte slice of size sz from the region
func (r *Region) alloc(sz int) []byte {
	report := shouldReport()
	if report {
		regionStats.WithLabelValues("alloc").Add(float64(samplerate))
	}
	if sz > pagesize { // slices that are too large come directly from the runtime
		if report {
			regionStats.WithLabelValues("alloc_too_large").Add(float64(samplerate))
		}
		return make([]byte, sz)
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	// because new pages are added at the end and older pages are nearly filled, we
	// iterate in reverse to increase the chance of finding a free page quickly
	for i := len(r.pages) - 1; i >= 0; i-- {
		used := r.used[i]
		// if enough space is left in this page, allocate using it
		if pagesize-used >= sz {
			ret := r.pages[i][used : used+sz : used+sz]
			r.used[i] += sz
			return ret
		}
	}
	// haven't found a page yet, so get a new page from the pool
	if report {
		regionStats.WithLabelValues("alloc_new_page").Add(float64(samplerate))
	}
	pg := pagepool.Get().([]byte)
	ret := pg[:sz:sz]
	r.pages = append(r.pages, pg)
	r.used = append(r.used, sz)
	return ret
}

// Free up all the memory used by this region
func (r *Region) Free() {
	r.lock.Lock()
	defer r.lock.Unlock()
	if shouldReport() {
		r.reportStats()
	}
	pages := r.pages
	r.pages = nil
	r.used = nil
	for _, pg := range pages {
		// zero out the slice and then put it back in the pool. This is critical especially
		// if it contained a pointer to another data structure. In such cases, failure to
		// free up memory can result in memory leaks.
		slice.Fill(pg, 0)
		pagepool.Put(pg)
	}
}

func (r *Region) reportStats() {
	regionSummaryStats.WithLabelValues("num_pages").Observe(float64(len(r.pages)))
	used, total := 0, 0
	for _, u := range r.used {
		used += u
		total += pagesize
	}
	regionSummaryStats.WithLabelValues("used_bytes").Observe(float64(used))
	regionSummaryStats.WithLabelValues("allocated_bytes").Observe(float64(total))
	useFraction := float64(0)
	if total > 0 {
		useFraction = float64(used) / float64(total)
	}
	regionSummaryStats.WithLabelValues("use_fraction").Observe(useFraction)
}

// Alloc allocates a struct of type T inside the region r
// This ideally would be a method on Region struct itself
// but go stupidly doesn't support generic types on methods
func Alloc[T any](region *Region) *T {
	var zero T
	sz := int(unsafe.Sizeof(zero))
	buf := region.alloc(sz)
	return (*T)(unsafe.Pointer(&buf))
}

// AllocSlice allocates a slice of type T inside the region.
// Note that it's possible to use this method to alloc a 2d
// slice for arbitrary type T. But in those cases, internal
// elements of the 2d slice are all empty and need to be allocated
// separately.
// This ideally would be a method on Region struct itself
// but go stupidly doesn't support generic types on methods
func AllocSlice[T any](region *Region, len_, cap_ int) []T {
	if cap_ < len_ {
		cap_ = len_
	}
	var zero T
	sz := int(unsafe.Sizeof(zero))
	buf := region.alloc(sz * cap_)
	slice := *(*[]T)(unsafe.Pointer(&buf))
	slice = slice[:len_]
	return slice
}
