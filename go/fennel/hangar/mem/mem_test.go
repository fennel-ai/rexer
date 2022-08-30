package mem

import (
	"encoding/binary"
	"fennel/hangar"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"
	"fmt"
	"go.uber.org/zap"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemStore(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		db, err := NewHangar(planeID, 1024, "", encoders.Default())
		assert.NoError(t, err)
		return db
	}
	skipped := []string{"test_concurrent"}
	hangar.TestStore(t, maker, skipped...)
}

func TestFullRun(t *testing.T) {
	logger, err := zap.NewDevelopment()
	zap.ReplaceGlobals(logger)
	assert.NoError(t, err)

	fsDir := t.TempDir()
	planeID := ftypes.RealmID(rand.Uint32())
	store, err := NewHangar(planeID, 1777, fsDir, encoders.Default())
	assert.NoError(t, err)

	testCount := 1_000_000

	for i := 0; i < testCount; i++ {
		key := make([]byte, 8)
		value := make([]byte, 16)
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(value, uint64(i))
		binary.BigEndian.PutUint64(value[8:], uint64(i))
		store.SimpleSet(key, value, 0)
		if i%100000 == 0 {
			zap.L().Info(fmt.Sprintf("progress %d", i))
		}
	}

	assert.Equal(t, testCount, store.Items())
	assert.Equal(t, 24*testCount, int(store.RawTotalSize()))

	err = store.Save()
	assert.NoError(t, err)
	_ = store.Close()
	assert.Equal(t, 0, store.Items())
	assert.Equal(t, 0, int(store.RawTotalSize()))

	store, err = NewHangar(planeID, 621, fsDir, encoders.Default())
	err = store.Load()
	assert.NoError(t, err)
	assert.Equal(t, testCount, store.Items())
	assert.Equal(t, 24*testCount, int(store.RawTotalSize()))
	_ = store.Close()
	assert.Equal(t, 0, store.Items())
	assert.Equal(t, 0, int(store.RawTotalSize()))

	// Intentionally create a new mem store with a different shard num
	// and make sure dumping & loading to the same directory won't be messed up by previous data
	store, err = NewHangar(planeID, 91, fsDir, encoders.Default())
	for i := 0; i < 1_000; i++ {
		key := make([]byte, 8)
		value := make([]byte, 16)
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(value, uint64(i))
		binary.BigEndian.PutUint64(value[8:], uint64(i))
		store.SimpleSet(key, value, time.Second*5)
		if i%100000 == 0 {
			zap.L().Info(fmt.Sprintf("progress %d", i))
		}
	}
	for i := 1000; i < 2_000; i++ {
		key := make([]byte, 8)
		value := make([]byte, 16)
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(value, uint64(i))
		binary.BigEndian.PutUint64(value[8:], uint64(i))
		store.SimpleSet(key, value, time.Second*1000)
		if i%100000 == 0 {
			zap.L().Info(fmt.Sprintf("progress %d", i))
		}
	}
	time.Sleep(time.Second * 6)
	err = store.Save() // expired items got purged by the "Save.." call
	assert.NoError(t, err)
	assert.Equal(t, 1000, store.Items())
	assert.Equal(t, 24*1000, int(store.RawTotalSize()))
	_ = store.Close()

	store, err = NewHangar(planeID, 91, fsDir, encoders.Default())
	err = store.Load()
	assert.NoError(t, err)
	assert.Equal(t, 1000, store.Items())
	assert.Equal(t, 24*1000, int(store.RawTotalSize()))
}

func BenchmarkMemStore(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.B) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		db, err := NewHangar(planeID, 1024, "", encoders.Default())
		assert.NoError(t, err)
		return db
	}
	hangar.BenchmarkStore(b, maker)
}
