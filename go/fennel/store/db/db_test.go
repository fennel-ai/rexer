package db

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/store/encoders"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDBStore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) store.Store {
		planeID := ftypes.RealmID(rand.Uint32())
		dirname := fmt.Sprintf("/tmp/badger_%d", planeID)
		db, err := NewStore(planeID, dirname, 10*1<<24, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	store.TestStore(t, maker)
}

var dummy int

func BenchmarkDBStore(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.B) store.Store {
		planeID := ftypes.RealmID(rand.Uint32())
		dirname := fmt.Sprintf("/tmp/badger_%d", planeID)
		db, err := NewStore(planeID, dirname, 10*1<<24, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	store.BenchmarkStore(b, maker)
}
