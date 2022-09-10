package binary

import (
	"math/rand"
	"testing"

	"fennel/lib/utils"

	"github.com/stretchr/testify/assert"
)

func TestPut_ReadString(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input string
		err   bool
		n     int
	}{
		{make([]byte, 0), "hello", true, 0},
		{make([]byte, 10), "hello", false, 6},
		{make([]byte, 256), utils.RandString(256), true, 0},
		{make([]byte, 500), utils.RandString(127), false, 127 + 1},
		{make([]byte, 500), utils.RandString(258), false, 258 + 2},
	}
	for _, scene := range scenarios {
		n, err := PutString(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.n, n)

			found, n1, err := ReadString(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, scene.n, n1)
		}
	}
}

func Test_Int64(t *testing.T) {
	for i := 0; i < 1000; i++ {
		x := rand.Int63()
		ret, err := Num2Bytes(x)
		assert.NoError(t, err)
		v, _, err := ParseInteger(ret)
		assert.NoError(t, err)
		assert.Equal(t, x, v)
	}
}

func TestPut_ReadVarint(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input int64
		err   bool
		n     int
	}{
		{make([]byte, 0), 1, true, 0},
		{make([]byte, 10), 1, false, 1},
		{make([]byte, 1), 255, true, 0},
		{make([]byte, 3), 255, false, 2},
		{make([]byte, 3), -255, false, 2},
	}
	for _, scene := range scenarios {
		n, err := PutVarint(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.n, n)

			found, n1, err := ReadVarint(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, scene.n, n1)
		}
	}
}

func TestPut_ReadUvarint(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input uint64
		err   bool
		n     int
	}{
		{make([]byte, 0), 1, true, 0},
		{make([]byte, 10), 1, false, 1},
		{make([]byte, 1), 255, true, 0},
		{make([]byte, 3), 255, false, 2},
	}
	for _, scene := range scenarios {
		n, err := PutUvarint(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.n, n)

			found, n1, err := ReadUvarint(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, scene.n, n1)
		}
	}
}

func TestPut_ReadBytes(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input []byte
		err   bool
		n     int
	}{
		{make([]byte, 0), []byte{'a'}, true, 0},
		{make([]byte, 1), []byte{'a', 'b'}, true, 0},
		// Extra byte needed for writing length.
		{make([]byte, 10), []byte{'a'}, false, 2},
		{make([]byte, 3), []byte{'a', 'b'}, false, 3},
	}
	for _, scene := range scenarios {
		n, err := PutBytes(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.n, n)

			found, n1, err := ReadBytes(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, scene.n, n1)
		}
	}
}

func TestPutUint64(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input uint64
		err   bool
	}{
		{make([]byte, 0), 1, true},
		{make([]byte, 7), 255, true},
		{make([]byte, 8), 255, false},
		{make([]byte, 8), 1 << 41, false},
		{make([]byte, 8), 1 << 41, false},
		{make([]byte, 10), rand.Uint64(), false},
	}
	for _, scene := range scenarios {
		n, err := PutUint64(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, 8, n)

			found, n1, err := ReadUint64(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, 8, n1)
		}
	}
}
func TestPutUint32(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input uint32
		err   bool
	}{
		{make([]byte, 0), 1, true},
		{make([]byte, 3), 255, true},
		{make([]byte, 4), 255, false},
		{make([]byte, 5), 255, false},
		{make([]byte, 4), rand.Uint32(), false},
	}
	for _, scene := range scenarios {
		n, err := PutUint32(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, 4, n)

			found, n1, err := ReadUint32(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, 4, n1)
		}
	}
}
func TestPutUint16(t *testing.T) {
	scenarios := []struct {
		buf   []byte
		input uint16
		err   bool
	}{
		{make([]byte, 0), 1, true},
		{make([]byte, 1), 255, true},
		{make([]byte, 2), 255, false},
		{make([]byte, 3), 255, false},
	}
	for _, scene := range scenarios {
		n, err := PutUint16(scene.buf, scene.input)
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, 2, n)

			found, n1, err := ReadUint16(scene.buf)
			assert.NoError(t, err)
			assert.Equal(t, scene.input, found)
			assert.Equal(t, 2, n1)
		}
	}
}
