package compress

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type TestStruct struct {
	A int
	B string
	C bool
}

func TestCompressStruct(t *testing.T) {
	val := &TestStruct{
		A: 1,
		B: "value",
		C: false,
	}
	b, err := Encode(val)
	assert.NoError(t, err)
	actualVal := &TestStruct{}
	assert.NoError(t, Decode(b, &actualVal))
	assert.Equal(t, val, actualVal)
}

func TestCompressSlice(t *testing.T) {
	val := make([]TestStruct, 2)
	for _, v := range val {
		v.A = 1
		v.B = "value"
		v.C = false
	}
	b, err := Encode(&val)
	assert.NoError(t, err)
	actualVal := make([]TestStruct, 2)
	assert.NoError(t, Decode(b, &actualVal))
	assert.Equal(t, val, actualVal)

}
