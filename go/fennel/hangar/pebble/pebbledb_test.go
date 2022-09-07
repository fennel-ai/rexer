package pebble

import (
	"math/rand"
	"testing"
	"time"

	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

func TestPebbleStore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		var opts *pebble.Options
		opts = opts.EnsureDefaults()
		db, err := NewHangar(planeID, t.TempDir(), opts, encoders.Default())
		assert.NoError(t, err)
		return db
	}
	skipped := []string{"test_set_ttl"}
	hangar.TestStore(t, maker, skipped...)
}
