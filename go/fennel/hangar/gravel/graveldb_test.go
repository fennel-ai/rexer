package gravel

import (
	"fennel/gravel"
	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGravelDB(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		opts := gravel.DefaultOptions()
		db, err := NewHangar(planeID, t.TempDir(), &opts, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	hangar.TestStore(t, maker)
}
