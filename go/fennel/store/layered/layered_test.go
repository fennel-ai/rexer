package layered

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/store/cache"
	"fennel/store/db"
	"fennel/store/encoders"
	"fmt"
	"io"
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

		return NewStore(planeID, cache, dbstore)
	}
	store.TestStore(t, maker)
}

func TestCaching(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	gt := mockStore{data: make(map[string]map[string]string)}

	// 80 MB cache with avg size of 100 bytes
	cache, err := cache.NewStore(planeID, 1<<23, 1000, encoders.Default())
	assert.NoError(t, err)

	s := NewStore(planeID, cache, gt)

	// first setup some keys
	key := store.Key{Data: []byte("key")}
	field1 := []byte("field1")
	field2 := []byte("field2")
	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")
	val4 := []byte("val4")

	gt.set(key.Data, field1, val1)
	gt.set(key.Data, field2, val2)
	ret, err := s.GetMany([]store.KeyGroup{{Prefix: key, Fields: [][]byte{field1, field2}}})
	time.Sleep(100 * time.Millisecond) // sleep a bit for writes to propagate
	assert.NoError(t, err)
	assert.Len(t, ret, 1)
	assert.ElementsMatch(t, [][]byte{field1, field2}, ret[0].Fields)
	assert.ElementsMatch(t, [][]byte{val1, val2}, ret[0].Values)

	// now update the fields
	gt.set(key.Data, field1, val3)
	gt.set(key.Data, field2, val4)

	// since data is cached, doing gets from store should be same as before
	ret, err = s.GetMany([]store.KeyGroup{{Prefix: key, Fields: [][]byte{field1, field2}}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{field1, field2}, ret[0].Fields)
	assert.ElementsMatch(t, [][]byte{val1, val2}, ret[0].Values)

	// now delete just one field
	assert.NoError(t, s.DelMany([]store.KeyGroup{{Prefix: key, Fields: [][]byte{field1}}}))
	time.Sleep(100 * time.Millisecond) // sleep a bit for writes to propagate

	// for that key alone the value should have changed
	ret, err = s.GetMany([]store.KeyGroup{{Prefix: key, Fields: [][]byte{field1, field2}}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{field1, field2}, ret[0].Fields)
	assert.ElementsMatch(t, [][]byte{val3, val2}, ret[0].Values)
}

type mockStore struct {
	planeID ftypes.RealmID
	data    map[string]map[string]string
}

func (m mockStore) PlaneID() ftypes.RealmID {
	return m.planeID
}

func (m mockStore) Encoder() store.Encoder { return nil }

func (m *mockStore) set(key, field, value []byte) {
	k := string(key)
	if _, ok := m.data[k]; !ok {
		m.data[k] = make(map[string]string)
	}
	m.data[k][string(field)] = string(value)
}

func (m mockStore) GetMany(kgs []store.KeyGroup) ([]store.ValGroup, error) {
	ret := make([]store.ValGroup, len(kgs))
	for i, kg := range kgs {
		prefix := string(kg.Prefix.Data)
		vg := &store.ValGroup{}
		if map_, ok := m.data[prefix]; ok {
			for _, f := range kg.Fields {
				if v, ok := map_[string(f)]; ok {
					vg.Fields = append(vg.Fields, f)
					vg.Values = append(vg.Values, []byte(v))
				}
			}
		}
		ret[i] = *vg
	}
	return ret, nil
}

func (m mockStore) SetMany(keys []store.Key, vgs []store.ValGroup) error { return nil }

func (m mockStore) DelMany(keys []store.KeyGroup) error { return nil }

func (m mockStore) Close() error { return nil }

func (m mockStore) Teardown() error { return nil }

func (m mockStore) Backup(sink io.Writer, since uint64) (uint64, error) { return 0, nil }

func (m mockStore) Restore(source io.Reader) error { return nil }

var _ store.Store = mockStore{}
