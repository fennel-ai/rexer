package encoders

import (
	"fennel/hangar"
	"fennel/lib/utils"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testEncodeKey(t *testing.T, enc hangar.Encoder) {
	scenarios := []struct {
		keys []hangar.Key
	}{
		{[]hangar.Key{
			{Data: []byte("hello")},
			{Data: []byte("great")},
			{Data: []byte{}},
			{Data: []byte(utils.RandString(10000))},
		}},
		{[]hangar.Key{}},
	}
	for _, scene := range scenarios {
		buf, err := hangar.EncodeKeyMany(scene.keys, enc)
		assert.NoError(t, err)
		for i := range scene.keys {
			var k hangar.Key
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

func testEncodeVal(t *testing.T, enc hangar.Encoder) {
	scenarios := []struct {
		vals  []hangar.ValGroup
		reuse bool
		err   bool
	}{
		{[]hangar.ValGroup{}, false, false},
		{[]hangar.ValGroup{}, true, false},
		{[]hangar.ValGroup{
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
		{[]hangar.ValGroup{
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
		{[]hangar.ValGroup{
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
		buf, err := hangar.EncodeValMany(scene.vals, enc)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			for i := range scene.vals {
				var v hangar.ValGroup
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

func benchmarkEncodeKey(b *testing.B, enc hangar.Encoder) {
	keys := make([]hangar.Key, 10_000)
	b.ReportAllocs()
	for i := range keys {
		keys[i].Data = []byte(utils.RandString(100))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := hangar.EncodeKeyMany(keys, enc)
		if err != nil {
			panic(err)
		}
	}
}

func benchmarkEncodeVals(b *testing.B, enc hangar.Encoder, numVG, numFields, szFields, szValues int) {
	vgs := make([]hangar.ValGroup, numVG)
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
		_, err := hangar.EncodeValMany(vgs, enc)
		if err != nil {
			panic(err)
		}
	}
}
