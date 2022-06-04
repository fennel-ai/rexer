package encoders

import (
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/store"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testEncodeKey(t *testing.T, enc store.Encoder) {
	scenarios := []struct {
		keys []store.Key
	}{
		{[]store.Key{
			{LShard: 0, TierID: 1231, Data: []byte("hello")},
			{LShard: 234, TierID: 0, Data: []byte("great")},
			{LShard: 255, TierID: 123, Data: []byte{}},
			{LShard: 13, TierID: 12341, Data: []byte(utils.RandString(10000))},
		}},
		{[]store.Key{}},
	}
	for _, scene := range scenarios {
		buf, err := store.EncodeKeyMany(scene.keys, enc)
		assert.NoError(t, err)
		for i := range scene.keys {
			var k store.Key
			_, err := enc.DecodeKey(buf[i], &k)
			assert.NoError(t, err)
			assert.Equal(t, scene.keys[i], k)
		}
		// also test encoding one by one
		arr := [1 << 20]byte{}
		for i := range scene.keys {
			dest := arr[:]
			n, err := enc.EncodeKey(dest, scene.keys[i])
			assert.NoError(t, err)
			assert.Equal(t, buf[i], dest[:n])
		}
	}
}

func testEncodeVal(t *testing.T, enc store.Encoder) {
	scenarios := []struct {
		vals  []store.ValGroup
		reuse bool
		err   bool
	}{
		{[]store.ValGroup{}, false, false},
		{[]store.ValGroup{}, true, false},
		{[]store.ValGroup{
			{
				Expiry: 1232,
				Fields: [][]byte{[]byte("foo"), []byte("bar"), []byte("foo"), []byte("baz"), []byte("foo"), []byte("qux")},
				Values: [][]byte{[]byte("foo"), []byte("bar"), []byte("foo"), []byte("baz"), []byte("foo"), []byte("qux")},
			},
			{
				Expiry: -1,
				Fields: [][]byte{[]byte("sdf"), []byte("sdf"), []byte("sdf"), []byte("sdf"), []byte("sdf"), []byte("sdf")},
				Values: [][]byte{[]byte("wqe"), []byte("wqe"), []byte("wqe"), []byte("wqe"), []byte("wqe"), []byte("wqe")},
			},
		}, true, false,
		},
		{[]store.ValGroup{
			{
				Expiry: 0,
				Fields: [][]byte{},
				Values: [][]byte{[]byte("foo")},
			},
			{
				Expiry: -1,
				Fields: [][]byte{[]byte("sdf"), []byte("sdf"), []byte("sdf"), []byte("sdf"), []byte("sdf"), []byte("sdf")},
				Values: [][]byte{[]byte("wqe"), []byte("wqe"), []byte("wqe"), []byte("wqe"), []byte("wqe"), []byte("wqe")},
			},
		}, true, true,
		},
		{[]store.ValGroup{
			{
				Expiry: 0,
				Fields: [][]byte{},
				Values: [][]byte{},
			},
			{
				Expiry: -1,
				Fields: [][]byte{[]byte(utils.RandString(100_000))},
				Values: [][]byte{[]byte(utils.RandString(100_000))},
			},
		}, true, false,
		},
	}
	for _, scene := range scenarios {
		buf, err := store.EncodeValMany(scene.vals, enc)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			for i := range scene.vals {
				var v store.ValGroup
				_, err := enc.DecodeVal(buf[i], &v, scene.reuse)
				assert.NoError(t, err)
				assert.Equal(t, scene.vals[i], v)
			}
			// also test encoding one by one
			arr := [1 << 20]byte{}
			for i := range scene.vals {
				dest := arr[:]
				n, err := enc.EncodeVal(dest, scene.vals[i])
				assert.NoError(t, err)
				assert.Equal(t, buf[i], dest[:n])
			}
		}
	}
}

func benchmarkEncodeKey(b *testing.B, enc store.Encoder) {
	keys := make([]store.Key, 10_000)
	b.ReportAllocs()
	for i := range keys {
		keys[i].LShard = byte(rand.Intn(256))
		keys[i].TierID = ftypes.RealmID(rand.Int63())
		keys[i].Data = []byte(utils.RandString(100))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.EncodeKeyMany(keys, enc)
		if err != nil {
			panic(err)
		}
	}
}

func benchmarkEncodeVals(b *testing.B, enc store.Encoder, numVG, numFields, szFields, szValues int) {
	vgs := make([]store.ValGroup, numVG)
	b.ReportAllocs()
	for i := range vgs {
		vgs[i].Expiry = int64(rand.Intn(30 * 24 * 3600))
		fields := make([][]byte, numFields)
		values := make([][]byte, numFields)
		for j := range fields {
			fields[j] = []byte(utils.RandString(szFields))
			values[j] = []byte(utils.RandString(szValues))
		}
		vgs[i].Fields = fields
		vgs[i].Values = values
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.EncodeValMany(vgs, enc)
		if err != nil {
			panic(err)
		}
	}
}
