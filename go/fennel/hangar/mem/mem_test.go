package mem

import (
	"math/rand"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"

	"github.com/stretchr/testify/assert"
)

func TestMem(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	maker := func(t *testing.T) hangar.Hangar {
		cache, err := NewHangar(planeID, 64, encoders.Default())
		assert.NoError(t, err)
		return cache
	}
	skipped := []string{"test_concurrent"}
	hangar.TestStore(t, maker, skipped...)
}
func BenchmarkMemStore(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.B) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		cache, err := NewHangar(planeID, 64, encoders.Default())
		assert.NoError(t, err)
		return cache
	}
	hangar.BenchmarkStore(b, maker)
}
