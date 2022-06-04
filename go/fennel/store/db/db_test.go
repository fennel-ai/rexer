package db

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/store/encoders"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

func TestDBStore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	maker := func(t *testing.T) store.Store {
		opts := badger.DefaultOptions(fmt.Sprintf("/tmp/badger_%d", planeID))
		opts = opts.WithLoggingLevel(badger.WARNING)
		opts = opts.WithBlockCacheSize(10 * 1 << 24) // 160MB
		db, err := NewStore(planeID, opts, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	store.TestStore(t, maker)
}

var dummy int

func BenchmarkDBStore(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	maker := func(t *testing.B) store.Store {
		opts := badger.DefaultOptions(fmt.Sprintf("/tmp/badger_%d", planeID))
		opts = opts.WithLoggingLevel(badger.WARNING)
		opts = opts.WithBlockCacheSize(10 * 1 << 24) // 16MB, so most reads come from disk
		db, err := NewStore(planeID, opts, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	store.BenchmarkStore(b, maker)
}
