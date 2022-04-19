package v1

import (
	"testing"

	"fennel/lib/utils/binary"
	"fennel/lib/value"

	"github.com/stretchr/testify/require"
)

func TestEncodeKey(t *testing.T) {
	codec := V1Codec{}
	encoded, err := codec.EncodeKey("otype", "1024", "mykey")
	require.NoError(t, err)

	isNext := func(encoded []byte, s string) int {
		buf := make([]byte, 32)
		otypelen, err := binary.PutString(buf, s)
		require.NoError(t, err)
		got, n, err := binary.ReadString(encoded[:otypelen])
		require.NoError(t, err)
		require.Equal(t, s, got)
		return n
	}

	// Check that the first byte is the codec identifier.
	curr := 0
	require.Equal(t, codec.Identifier(), encoded[curr])
	curr++
	// Next should be the otype, oid, and key.
	curr += isNext(encoded[curr:], "otype")
	curr += isNext(encoded[curr:], "1024")
	curr += isNext(encoded[curr:], "mykey") // nolint
}

func TestValueEagerDecode(t *testing.T) {
	codec := V1Codec{}

	version := uint64(1)
	value := value.NewDict(map[string]value.Value{
		"a": value.Int(5),
	})
	v, err := codec.EncodeValue(1, value)
	require.NoError(t, err)

	decoded, err := codec.EagerDecode(v)
	require.NoError(t, err)
	gotversion, err := decoded.UpdateTime()
	require.NoError(t, err)
	require.Equal(t, gotversion, version)
	gotvalue, err := decoded.Value()
	require.NoError(t, err)
	require.True(t, value.Equal(gotvalue))
}

func TestValueLazyDecode(t *testing.T) {
	codec := V1Codec{}

	version := uint64(1)
	value := value.NewDict(map[string]value.Value{
		"a": value.Int(5),
	})
	v, err := codec.EncodeValue(1, value)
	require.NoError(t, err)

	// Initialize lazy decoder.
	decoded, err := codec.LazyDecode(v)
	require.NoError(t, err)
	require.IsType(t, &lazilyDecodedValue{}, decoded)
	// Get update time from encoded value.
	gotversion, err := decoded.UpdateTime()
	require.NoError(t, err)
	require.Equal(t, gotversion, version)
	// Test that decoding is lazy.
	lazydecoder, ok := decoded.(*lazilyDecodedValue)
	require.True(t, ok)
	require.NotEqual(t, len(lazydecoder.raw), lazydecoder.idx)
	// Now get value.
	gotvalue, err := decoded.Value()
	require.NoError(t, err)
	require.True(t, value.Equal(gotvalue))

	// Getting value first and then UpdateTime from lazily decoded value is also OK.
	decoded, err = codec.LazyDecode(v)
	require.NoError(t, err)
	gotvalue, err = decoded.Value()
	require.NoError(t, err)
	require.True(t, value.Equal(gotvalue))
	gotversion, err = decoded.UpdateTime()
	require.NoError(t, err)
	require.Equal(t, gotversion, version)
}

func TestDecodeFail(t *testing.T) {
	codec := V1Codec{}
	// Sending gibberish to decode should fail.
	_, err := codec.EagerDecode([]byte{'a'})
	require.Error(t, err)
}
