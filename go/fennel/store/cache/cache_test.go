package cache

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/store/encoders"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	maker := func(t *testing.T) store.Store {
		// 80 MB cache with avg size of 100 bytes
		cache, err := NewStore(planeID, 1<<23, 1000, encoders.Default())
		assert.NoError(t, err)
		return cache
	}
	store.TestStore(t, maker)
}
