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
	for i := 0; i < 100; i++ {
		x := int64(rand.Int())
		if rand.Int()%2 == 0 {
			x = -x
		}
		ret, err := Num2Bytes(x)
		assert.NoError(t, err)
		assert.Equal(t, x, ParseInteger(ret))
	}
}

func Test_Float64(t *testing.T) {
	for i := 0; i < 100; i++ {
		x := rand.Float32() * 1000
		if rand.Int()%2 == 0 {
			x = -x
		}
		ret, err := Num2Bytes(float64(x))
		assert.NoError(t, err)
		assert.Equal(t, x, float32(ParseFloat(ret)))
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
