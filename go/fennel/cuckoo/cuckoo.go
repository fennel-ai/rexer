package cuckoo

import (
	"fennel/lib/utils/math"
	"math/rand"
)

// maxCuckooKickouts is the maximum number of times reinsert is attempted
const maxCuckooKickouts = 10_000

type Filter[T any] struct {
	buckets []bucket[T]
	modulo  uint64
	count   uint64
}

// NewFilter returns a new cuckoofilter suitable for the given number of elements.
// When inserting more elements, insertion speed will drop significantly and insertions might fail altogether.
func NewFilter[T any](numElements uint) *Filter[T] {
	numBuckets := math.NextPowerOf2(uint64(numElements / bucketSize))
	buckets := make([]bucket[T], numBuckets)
	return &Filter[T]{
		buckets: buckets,
		count:   0,
		modulo:  uint64(len(buckets) - 1),
	}
}

// Lookup returns true if data is in the filter.
func (cf *Filter[T]) Lookup(h uint64) (T, bool) {
	i1, fp := getIndexFingerprint(h, cf.modulo)
	if data, has := cf.buckets[i1].get(fp); has {
		return data, has
	}
	i2 := getAltIndex(fp, i1, cf.modulo)
	return cf.buckets[i2].get(fp)
}

// Insert data into the filter. Returns false if insertion failed. In the resulting state, the filter
// * Might return false negatives
// * Deletes are not guaranteed to work
// To increase success rate of inserts, create a larger filter.
func (cf *Filter[T]) Insert(h uint64, data T) bool {
	i1, fp := getIndexFingerprint(h, cf.modulo)
	if cf.insert(data, fp, i1) {
		return true
	}
	i2 := getAltIndex(fp, i1, cf.modulo)
	if cf.insert(data, fp, i2) {
		return true
	}
	return cf.reinsert(data, fp, randi(i1, i2))
}

func (cf *Filter[T]) insert(data T, fp fingerprint, i uint) bool {
	if cf.buckets[i].insert(fp, data) {
		cf.count++
		return true
	}
	return false
}

func (cf *Filter[T]) reinsert(data T, fp fingerprint, i uint) bool {
	e := entry[T]{data: data, fp: fp}
	for k := 0; k < maxCuckooKickouts; k++ {
		j := rand.Intn(bucketSize)
		// Swap entry with bucket entry.
		cf.buckets[i][j], e = e, cf.buckets[i][j]

		// Move kicked out entry to alternate location.
		i = getAltIndex(e.fp, i, cf.modulo)
		if cf.insert(e.data, e.fp, i) {
			return true
		}
	}
	return false
}

// Delete data from the filter. Returns true if the data was found and deleted.
func (cf *Filter[T]) Delete(h uint64) bool {
	i1, fp := getIndexFingerprint(h, cf.modulo)
	i2 := getAltIndex(fp, i1, cf.modulo)
	return cf.delete(fp, i1) || cf.delete(fp, i2)
}

func (cf *Filter[T]) delete(fp fingerprint, i uint) bool {
	if cf.buckets[i].delete(fp) {
		cf.count--
		return true
	}
	return false
}

// Count returns the number of items in the filter.
func (cf *Filter[T]) Count() uint64 {
	return cf.count
}

// LoadFactor returns the fraction slots that are occupied.
func (cf *Filter[T]) LoadFactor() float64 {
	return float64(cf.count) / float64(len(cf.buckets)*bucketSize)
}

const bytesPerBucket = bucketSize * fingerprintSizeBits / 8
