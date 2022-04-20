package v1

import (
	"testing"

	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/value"

	"github.com/stretchr/testify/require"
)

func TestEncodeKey(t *testing.T) {
	codec := V1Codec{}
	encoded, err := codec.EncodeKey(ftypes.AggId(13), counter.Bucket{
		Key:    "12",
		Window: 123,
		Width:  1,
		Index:  100,
	})
	require.NoError(t, err)

	isNext := func(encoded []byte, s string) int {
		buf := make([]byte, 32)
		len, err := binary.PutString(buf, s)
		require.NoError(t, err)
		got, n, err := binary.ReadString(encoded[:len])
		require.NoError(t, err)
		require.Equal(t, s, got)
		return n
	}

	isUintNext := func(encoded []byte, i uint64) int {
		buf := make([]byte, 32)
		len, err := binary.PutUvarint(buf, i)
		require.NoError(t, err)
		got, n, err := binary.ReadUvarint(encoded[:len])
		require.NoError(t, err)
		require.Equal(t, i, got)
		return n
	}

	// Check that the first byte is the codec identifier.
	curr := 0
	require.Equal(t, codec.Identifier(), encoded[curr])
	curr++

	curr += isNext(encoded[curr:], "12")
	curr += isUintNext(encoded[curr:], 123)
	curr += isUintNext(encoded[curr:], 1)
	curr += isUintNext(encoded[curr:], 100)
	curr += isUintNext(encoded[curr:], 13)
}

func TestValueDecode(t *testing.T) {
	codec := V1Codec{}
	value := value.NewDict(map[string]value.Value{
		"a": value.Int(5),
	})
	v, err := codec.EncodeValue(value)
	require.NoError(t, err)

	decoded, err := codec.DecodeValue(v)
	require.NoError(t, err)
	require.Equal(t, value, decoded)
}

func TestDecodeFail(t *testing.T) {
	codec := V1Codec{}
	// Sending gibberish to decode should fail.
	_, err := codec.DecodeValue([]byte{'a'})
	require.Error(t, err)
}
