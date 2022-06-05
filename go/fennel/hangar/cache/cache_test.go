package cache

import (
	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	maker := func(t *testing.T) hangar.Hangar {
		// 80 MB cache with avg size of 100 bytes
		cache, err := NewHangar(planeID, 1<<23, 1000, encoders.Default())
		assert.NoError(t, err)
		return cache
	}
	hangar.TestStore(t, maker)
}
func BenchmarkCacheStore(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.B) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		cache, err := NewHangar(planeID, 1<<23, 1000, encoders.Default())
		assert.NoError(t, err)
		return cache
	}
	hangar.BenchmarkStore(b, maker)
}
