package db

import (
	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDBStore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		dirname := t.TempDir()
		db, err := NewHangar(planeID, dirname, 10*1<<24, encoders.Default())
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
		db, err := NewHangar(planeID, dirname, 10*1<<24, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	hangar.BenchmarkStore(b, maker)
}
