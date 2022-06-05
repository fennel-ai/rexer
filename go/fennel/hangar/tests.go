package hangar

import (
	"fennel/lib/utils"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStore(t *testing.T, maker func(t *testing.T) Hangar) {
	scenarios := []struct {
		name string
		test func(t *testing.T, store Hangar)
	}{
		{name: "test_basic", test: testBasic},
		{name: "test_set_ttl", test: testTTL},
		{name: "test_partial_missing", test: testPartialMissing},
		{name: "test_large_batch", test: testLargeBatch},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			store := maker(t)
			defer store.Teardown()
			scenario.test(t, store)
		})
	}
}

func BenchmarkStore(b *testing.B, maker func(b *testing.B) Hangar) {
	b.Run("basic:keys_num_10000_sz_100:fields_num_100:vals_sz_100:gets_1000", func(b *testing.B) {
		store := maker(b)
		defer store.Teardown()
		benchmarkGetSet(b, store, 10000, 100, 100, 100, 1000)
	})
	b.Run("basic:keys_num_10000_sz_100:fields_num_100:vals_sz_100:gets_1000", func(b *testing.B) {
		store := maker(b)
		defer store.Teardown()
		benchmarkGetSet(b, store, 10000, 10, 100, 100, 1000)
	})
}

func testBasic(t *testing.T, store Hangar) {
	keys, kgs, vgs := getData(3, 5)
	// initially all empty
	verifyMissing(t, store, kgs)

	// set all with infinite ttl and verify can get
	err := store.SetMany(keys, vgs)
	assert.NoError(t, err)
	verifyValues(t, store, kgs, vgs)

	// now delete all and verify missing
	err = store.DelMany(kgs)
	assert.NoError(t, err)
	verifyMissing(t, store, kgs)
}

func testTTL(t *testing.T, store Hangar) {
	keys, kgs, vgs := getData(3, 5)
	// initially all empty
	verifyMissing(t, store, kgs)
	// set all a TTL of 1 second
	for i := range vgs {
		(&vgs[i]).Expiry = int64(time.Now().Unix()) + 1
	}
	// set with TTL and verify can get
	err := store.SetMany(keys, vgs)
	assert.NoError(t, err)
	verifyValues(t, store, kgs, vgs)

	// now sleep for 2 seconds and verify missing
	time.Sleep(time.Second * 2)
	verifyMissing(t, store, kgs)
}

func testPartialMissing(t *testing.T, store Hangar) {
	keys, kgs, vgs := getData(10, 10)

	// set all and verify can get
	err := store.SetMany(keys, vgs)
	assert.NoError(t, err)
	verifyValues(t, store, kgs, vgs)

	oddKg, evenKg := make([]KeyGroup, 10), make([]KeyGroup, 10)
	oddVg, evenVg := make([]ValGroup, 10), make([]ValGroup, 10)
	for i, kg := range kgs {
		oddKg[i].Prefix = kg.Prefix
		evenKg[i].Prefix = kg.Prefix
		for j, field := range kg.Fields {
			if (i+j)%2 == 0 {
				evenKg[i].Fields = append(evenKg[i].Fields, field)
				evenVg[i].Fields = append(evenVg[i].Fields, vgs[i].Fields[j])
				evenVg[i].Values = append(evenVg[i].Values, vgs[i].Values[j])
			} else {
				oddKg[i].Fields = append(oddKg[i].Fields, field)
				oddVg[i].Fields = append(oddVg[i].Fields, vgs[i].Fields[j])
				oddVg[i].Values = append(oddVg[i].Values, vgs[i].Values[j])
			}
		}
	}

	// now delete half and verify can get the rest
	err = store.DelMany(oddKg)
	assert.NoError(t, err)
	verifyMissing(t, store, oddKg)
	verifyValues(t, store, evenKg, evenVg)
}

func testLargeBatch(t *testing.T, store Hangar) {
	keys, kgs, vgs := getData(65, 3)
	verifyMissing(t, store, kgs)
	err := store.SetMany(keys, vgs)
	assert.NoError(t, err)
	verifyValues(t, store, kgs, vgs)
	assert.NoError(t, store.DelMany(kgs))
	verifyMissing(t, store, kgs)
}

func verifyValues(t *testing.T, store Hangar, kgs []KeyGroup, vgs []ValGroup) {
	// sleep for a bit to ensure all writes are flushed
	time.Sleep(100 * time.Millisecond)
	found, err := store.GetMany(kgs)
	assert.NoError(t, err)
	assert.Len(t, found, len(kgs))
	for i := range found {
		assert.Equal(t, vgs[i], found[i])
	}
}

func verifyMissing(t *testing.T, store Hangar, kgs []KeyGroup) {
	time.Sleep(100 * time.Millisecond)
	found, err := store.GetMany(kgs)
	assert.NoError(t, err)
	assert.Len(t, found, len(kgs))
	for i, kg := range kgs {
		for _, field := range kg.Fields {
			assert.NotContains(t, found[i].Fields, field)
		}
	}
}

func getData(numKey, numIndex int) ([]Key, []KeyGroup, []ValGroup) {
	keys := make([]Key, numKey)
	kgs := make([]KeyGroup, numKey)
	vgs := make([]ValGroup, numKey)
	fields := make(Fields, numIndex)
	for i := range fields {
		fields[i] = []byte(fmt.Sprintf("field%d", i))
	}
	for i := range keys {
		keys[i] = Key{Data: []byte(utils.RandString(10))}
		kgs[i] = KeyGroup{
			Prefix: keys[i],
			Fields: fields,
		}
		vgs[i] = ValGroup{
			Fields: fields,
		}
		for j := range kgs[i].Fields {
			vgs[i].Values = append(vgs[i].Values, []byte(fmt.Sprintf("value%d", i*numIndex+j)))
		}
	}
	return keys, kgs, vgs
}

var dummy int

func benchmarkGetSet(b *testing.B, store Hangar, numKeys, numFields, szKey, szVal, szGets int) {
	b.ReportAllocs()
	// first create all the key/field/value data
	keys := make([]Key, numKeys)
	fields := make([][][]byte, numKeys)
	vals := make([][][]byte, numKeys)
	vgs := make([]ValGroup, numKeys)
	for i := range keys {
		keys[i].Data = []byte(utils.RandString(szKey))
		for j := 0; j < numFields; j++ {
			fields[i] = append(fields[i], []byte(fmt.Sprintf("%d", j)))
			vals[i] = append(vals[i], []byte(utils.RandString(szVal)))
		}
		vgs[i].Values = vals[i]
		vgs[i].Fields = fields[i]
	}
	// and set this
	assert.NoError(b, store.SetMany(keys, vgs))
	ratio := numKeys / szGets
	toRead := make([]KeyGroup, szGets)

	// reset the timer so that we don't include the setup path
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// stop timer for request specific setup
		cur := 0
		for start := rand.Intn(szGets); cur < len(toRead) && start < len(keys); cur++ {
			toRead[cur].Prefix = keys[start]
			toRead[cur].Fields = fields[start]
			start += ratio
		}
		b.StartTimer()
		_, err := store.GetMany(toRead[:cur])
		if err != nil {
			panic(err)
		}
	}
}
