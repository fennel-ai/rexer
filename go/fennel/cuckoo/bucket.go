package cuckoo

type fingerprint uint8

type entry[T any] struct {
	fp   fingerprint
	data T
}

const (
	nullFp              = 0
	bucketSize          = 4
	fingerprintSizeBits = 8
	maxFingerprint      = (1 << fingerprintSizeBits) - 1
)

type bucket[T any] [bucketSize]entry[T]

// insert a fingerprint into a bucket. Returns true if there was enough space and insertion succeeded.
// Note it allows inserting the same fingerprint multiple times.
func (b *bucket[T]) insert(fp fingerprint, data T) bool {
	for i, e := range b {
		if e.fp == fp {
			e.data = data
			return true
		}
		if e.fp == nullFp {
			b[i].fp = fp
			b[i].data = data
			return true
		}
	}
	return false
}

// delete a fingerprint from a bucket.
// Returns true if the fingerprint was present and successfully removed.
func (b *bucket[T]) delete(fp fingerprint) bool {
	for _, e := range b {
		if e.fp == fp {
			e.fp = nullFp
			return true
		}
	}
	return false
}

func (b *bucket[T]) get(needle fingerprint) (T, bool) {
	for _, e := range b {
		if e.fp == needle {
			return e.data, true
		}
	}
	var zero T
	return zero, false
}
