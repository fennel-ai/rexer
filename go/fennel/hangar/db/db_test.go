package db

import (
	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"
	"math/rand"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

func TestDBStore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		db, err := NewHangar(planeID, badger.DefaultOptions(t.TempDir()), encoders.Default())
		assert.NoError(t, err)
		return db
	}
	hangar.TestStore(t, maker)
}

func BenchmarkDBStore(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.B) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		dirname := t.TempDir()
		db, err := NewHangar(planeID, badger.DefaultOptions(dirname), encoders.Default())
		assert.NoError(t, err)
		return db
	}
	hangar.BenchmarkStore(b, maker)
}
