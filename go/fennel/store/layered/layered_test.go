package layered

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/store/cache"
	"fennel/store/db"
	"fennel/store/encoders"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLayered_Cache_DB(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	maker := func(t *testing.T) store.Store {
		dirname := fmt.Sprintf("/tmp/badger_%d", planeID)
		dbstore, err := db.NewStore(planeID, dirname, 10*1<<24, encoders.Default())
		assert.NoError(t, err)

		// 80 MB cache with avg size of 100 bytes
		cache, err := cache.NewStore(planeID, 1<<23, 1000, encoders.Default())
		assert.NoError(t, err)

		return NewLayered(planeID, cache, dbstore)
	}
	store.TestStore(t, maker)
}
