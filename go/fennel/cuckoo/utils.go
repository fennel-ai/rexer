package cuckoo

import (
	"github.com/cespare/xxhash/v2"
	"github.com/detailyang/fastrand-go"
)

// randi returns either i1 or i2 randomly.
func randi(i1, i2 uint) uint {
	if fastrand.FastRand()&1 == 0 {
		return i1
	}
	return i2
}

func getAltIndex(fp fingerprint, i uint, modulo uint64) uint {
	b := make([]byte, 1)
	b[0] = uint8(fp)
	hash := xxhash.Sum64(b)
	return uint((uint64(i) ^ hash) & modulo)
}

func getIndexFingerprint(h uint64, modulo uint64) (uint, fingerprint) {
	i1 := uint(h & modulo)                             // lowest order bits
	fp := fingerprint(h >> (64 - fingerprintSizeBits)) // top most 8 bits

	// Valid fingerprints are in range [1, maxFingerprint], leaving 0 as the special empty state.
	fp = fp%(maxFingerprint-1) + 1
	return i1, fp
}
