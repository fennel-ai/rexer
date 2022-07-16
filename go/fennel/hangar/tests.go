package hangar

import (
	"fennel/lib/utils"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/samber/mo"
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
		{name: "test_select_all", test: testSelectAll},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			store := maker(t)
			defer func() { _ = store.Teardown() }()
			scenario.test(t, store)
		})
	}
}

func BenchmarkStore(b *testing.B, maker func(b *testing.B) Hangar) {
	b.Run("basic:keys_num_10000_sz_100:fields_num_100:vals_sz_100:gets_1000", func(b *testing.B) {
		store := maker(b)
		defer func() { _ = store.Teardown() }()
		benchmarkGetSet(b, store, 10000, 100, 100, 100, 1000)
	})
	b.Run("basic:keys_num_10000_sz_100:fields_num_100:vals_sz_100:gets_1000", func(b *testing.B) {
		store := maker(b)
		defer func() { _ = store.Teardown() }()
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
	keys, kgs, vgs := getData(100, 5)
	// initially all empty
	verifyMissing(t, store, kgs)
	// set first 50 keys with a large TTL of 10 minutes and the last 50 keys
	// with a small TTL of 1 second.
	start := time.Now()
	for i := range vgs[:50] {
		if i > 50 {
			(&vgs[i]).Expiry = start.Unix() + 600
		} else {
			(&vgs[i]).Expiry = start.Unix() + 1
		}
	}
	err := store.SetMany(keys, vgs)
	assert.NoError(t, err)
	// sleep 2 seconds and verify we can only get the last 50 keys.
	time.Sleep(2 * time.Second)
	verifyMissing(t, store, kgs[0:50])
	verifyValues(t, store, kgs[50:], vgs[50:])
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
		var oddfields Fields
		var evenfields Fields
		for j, field := range kg.Fields.OrEmpty() {
			if (i+j)%2 == 0 {
				evenfields = append(evenfields, field)
				evenVg[i].Fields = append(evenVg[i].Fields, vgs[i].Fields[j])
				evenVg[i].Values = append(evenVg[i].Values, vgs[i].Values[j])
			} else {
				oddfields = append(oddfields, field)
				oddVg[i].Fields = append(oddVg[i].Fields, vgs[i].Fields[j])
				oddVg[i].Values = append(oddVg[i].Values, vgs[i].Values[j])
			}
		}
		oddKg[i] = KeyGroup{
			Prefix: kg.Prefix,
			Fields: mo.Some(oddfields),
		}
		evenKg[i] = KeyGroup{
			Prefix: kg.Prefix,
			Fields: mo.Some(evenfields),
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

func testSelectAll(t *testing.T, store Hangar) {
	keys, kgs, vgs := getData(10, 20)
	verifyMissing(t, store, kgs)
	err := store.SetMany(keys, vgs)
	assert.NoError(t, err)
	verifyValues(t, store, kgs, vgs)
	// Key-groups with fields no specified.
	kgsNoFields := make([]KeyGroup, 10)
	for i, kg := range kgs {
		kgsNoFields[i] = KeyGroup{
			Prefix: kg.Prefix,
		}
	}
	verifyValues(t, store, kgsNoFields, vgs)
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
		for _, field := range kg.Fields.OrEmpty() {
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
			Fields: mo.Some(fields),
		}
		vgs[i] = ValGroup{
			Fields: fields,
		}
		for j := range kgs[i].Fields.OrEmpty() {
			vgs[i].Values = append(vgs[i].Values, []byte(fmt.Sprintf("value%d", i*numIndex+j)))
		}
	}
	return keys, kgs, vgs
}

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
			toRead[cur].Fields = mo.Some[Fields](fields[start])
			start += ratio
		}
		b.StartTimer()
		_, err := store.GetMany(toRead[:cur])
		if err != nil {
			panic(err)
		}
	}
}
