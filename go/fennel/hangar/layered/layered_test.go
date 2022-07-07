package layered

import (
	"fennel/hangar"
	"fennel/hangar/cache"
	"fennel/hangar/db"
	"fennel/hangar/encoders"
	"fennel/lib/ftypes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestLayered_Store(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.T) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		dirname := fmt.Sprintf("/tmp/badger/%d", planeID)
		dbstore, err := db.NewHangar(planeID, dirname, 64*1<<20, encoders.Default())
		assert.NoError(t, err)

		// 80 MB cache with avg size of 100 bytes
		cache, err := cache.NewHangar(planeID, 1<<23, 1000, encoders.Default())
		assert.NoError(t, err)

		return NewHangar(planeID, cache, dbstore)
	}
	hangar.TestStore(t, maker)
}

func BenchmarkLayered_GetMany(b *testing.B) {
	rand.Seed(time.Now().UnixNano())
	maker := func(t *testing.B) hangar.Hangar {
		planeID := ftypes.RealmID(rand.Uint32())
		dirname := fmt.Sprintf("/tmp/badger/%d", planeID)
		// 160MB block cache
		dbstore, err := db.NewHangar(planeID, dirname, 1<<20, encoders.Default())
		assert.NoError(t, err)
		// 80 MB cache with avg size of 100 bytes
		cache, err := cache.NewHangar(planeID, 1<<23, 1000, encoders.Default())
		assert.NoError(t, err)
		return NewHangar(planeID, cache, dbstore)
	}
	hangar.BenchmarkStore(b, maker)
}

func TestCaching(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.RealmID(rand.Uint32())
	gt := &mockStore{data: make(map[string]map[string]string)}

	// 80 MB cache with avg size of 100 bytes
	cache, err := cache.NewHangar(planeID, 1<<23, 1000, encoders.Default())
	assert.NoError(t, err)

	s := NewHangar(planeID, cache, gt)
	defer s.Teardown() //nolint: errcheck

	// first setup some keys
	key := hangar.Key{Data: []byte("key")}
	field1 := []byte("field1")
	field2 := []byte("field2")
	val1 := []byte("val1")
	val2 := []byte("val2")
	val3 := []byte("val3")
	val4 := []byte("val4")

	gt.set(key.Data, field1, val1)
	gt.set(key.Data, field2, val2)
	ret, err := s.GetMany([]hangar.KeyGroup{{Prefix: key, Fields: mo.Some[hangar.Fields]([][]byte{field1, field2})}})
	time.Sleep(100 * time.Millisecond) // sleep a bit for writes to propagate
	assert.NoError(t, err)
	assert.Len(t, ret, 1)
	assert.ElementsMatch(t, [][]byte{field1, field2}, ret[0].Fields)
	assert.ElementsMatch(t, [][]byte{val1, val2}, ret[0].Values)

	// now update the fields
	gt.set(key.Data, field1, val3)
	gt.set(key.Data, field2, val4)

	// since data is cached, doing gets from store should be same as before
	ret, err = s.GetMany([]hangar.KeyGroup{{Prefix: key, Fields: mo.Some[hangar.Fields]([][]byte{field1, field2})}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{field1, field2}, ret[0].Fields)
	assert.ElementsMatch(t, [][]byte{val1, val2}, ret[0].Values)

	// now delete just one field
	assert.NoError(t, s.DelMany([]hangar.KeyGroup{{Prefix: key, Fields: mo.Some[hangar.Fields]([][]byte{field1})}}))
	time.Sleep(100 * time.Millisecond) // sleep a bit for writes to propagate

	// for that key alone the value should have changed
	ret, err = s.GetMany([]hangar.KeyGroup{{Prefix: key, Fields: mo.Some[hangar.Fields]([][]byte{field1, field2})}})
	assert.NoError(t, err)
	assert.ElementsMatch(t, [][]byte{field1, field2}, ret[0].Fields)
	assert.ElementsMatch(t, [][]byte{val3, val2}, ret[0].Values)
}

type mockStore struct {
	planeID ftypes.RealmID
	data    map[string]map[string]string
	sync.Mutex
}

func (m *mockStore) PlaneID() ftypes.RealmID {
	return m.planeID
}

func (m *mockStore) Encoder() hangar.Encoder { return nil }

func (m *mockStore) set(key, field, value []byte) {
	m.Lock()
	defer m.Unlock()
	k := string(key)
	if _, ok := m.data[k]; !ok {
		m.data[k] = make(map[string]string)
	}
	m.data[k][string(field)] = string(value)
}

func (m *mockStore) GetMany(kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	m.Lock()
	defer m.Unlock()
	ret := make([]hangar.ValGroup, len(kgs))
	for i, kg := range kgs {
		vg := &hangar.ValGroup{}
		if map_, ok := m.data[string(kg.Prefix.Data)]; ok {
			if kg.Fields.IsAbsent() {
				for f, v := range map_ {
					vg.Fields = append(vg.Fields, []byte(f))
					vg.Values = append(vg.Values, []byte(v))
				}
			} else {
				for _, f := range kg.Fields.OrEmpty() {
					if v, ok := map_[string(f)]; ok {
						vg.Fields = append(vg.Fields, f)
						vg.Values = append(vg.Values, []byte(v))
					}
				}
			}
		}
		ret[i] = *vg
	}
	return ret, nil
}

func (m *mockStore) SetMany(keys []hangar.Key, vgs []hangar.ValGroup) error { return nil }

func (m *mockStore) DelMany(keys []hangar.KeyGroup) error { return nil }

func (m *mockStore) Close() error { return nil }

func (m *mockStore) Teardown() error { return nil }

func (m *mockStore) Backup(sink io.Writer, since uint64) (uint64, error) { return 0, nil }

func (m *mockStore) Restore(source io.Reader) error { return nil }

var _ hangar.Hangar = &mockStore{}
