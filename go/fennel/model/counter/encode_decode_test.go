package counter

import (
	"fennel/lib/codex"
	"fennel/lib/utils/binary"
	"fmt"
	"testing"

	"github.com/mtraver/base91"
	"github.com/stretchr/testify/assert"
)

func TestRedisKeyPrefixCodec(t *testing.T) {
	var codec codex.Codex = 1
	buf := make([]byte, 8)
	curr := 0
	n, err := codec.Write(buf[curr:])
	assert.NoError(t, err)
	curr += n
	fmt.Printf("str: %s\n", base91.StdEncoding.EncodeToString(buf[:curr]))
}

func TestRedisKeyPrefixAggId(t *testing.T) {
	buf := make([]byte, 8)
	curr := 0
	n, err := binary.PutUvarint(buf[curr:], uint64(21))
	assert.NoError(t, err)
	curr += n
	fmt.Printf("str: %s\n", base91.StdEncoding.EncodeToString(buf[:curr]))
}

func TestRedisKeyPrefixAggIdDecode(t *testing.T) {
	s := "LA"
	b, err := base91.StdEncoding.DecodeString(s)
	assert.NoError(t, err)
	expected, n, err := binary.ReadUvarint(b)
	assert.NoError(t, err)
	b = b[n:]
	fmt.Printf("str: %s, aggId: %d\n", s, expected)
}

func TestRedisKeyPrefixDecode(t *testing.T) {
	b, err := base91.StdEncoding.DecodeString("CtA")
	assert.NoError(t, err)
	var codec codex.Codex = 1
	expected, n, err := codex.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, codec, expected)
	b = b[n:]

	expectedn, x, err := binary.ReadUvarint(b)
	assert.NoError(t, err)
	b = b[x:]
	fmt.Printf("aggId: %d\n", expectedn)
}

func TestRedisKeyPrefix(t *testing.T) {
	var codec codex.Codex = 1
	buf := make([]byte, 8+8)
	curr := 0
	n, err := codec.Write(buf[curr:])
	assert.NoError(t, err)
	curr += n

	actualId := 31
	n, err = binary.PutUvarint(buf[curr:], uint64(actualId))
	assert.NoError(t, err)
	curr += n

	s := base91.StdEncoding.EncodeToString(buf[:curr])
	fmt.Printf("str: %s\n", s)

	b, err := base91.StdEncoding.DecodeString(s)
	assert.NoError(t, err)
	expected, n, err := codex.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, codec, expected)
	b = b[n:]

	expectedn, x, err := binary.ReadUvarint(b)
	assert.NoError(t, err)
	b = b[x:]
	fmt.Printf("aggId: %d, actual: %d\n", expectedn, actualId)
}

func TestRedisKeyPrefixString(t *testing.T) {
	var codec codex.Codex = 1
	buf := make([]byte, 8+8+8+8)
	curr := 0
	n, err := codec.Write(buf[curr:])
	assert.NoError(t, err)
	curr += n
	actualId := 31
	n, err = binary.PutUvarint(buf[curr:], uint64(actualId))
	assert.NoError(t, err)
	curr += n
	actuals := "coobar"
	n, err = binary.PutString(buf[curr:], "coobar")
	assert.NoError(t, err)
	curr += n

	s := base91.StdEncoding.EncodeToString(buf[:curr])
	fmt.Printf("str: %s\n", s)

	b, err := base91.StdEncoding.DecodeString(s)
	assert.NoError(t, err)
	expected, n, err := codex.Read(b)
	assert.NoError(t, err)
	assert.Equal(t, codec, expected)
	b = b[n:]

	expectedn, x, err := binary.ReadUvarint(b)
	assert.NoError(t, err)
	b = b[x:]
	fmt.Printf("aggId: %d, actual: %d\n", expectedn, actualId)

	expecteds, y, err := binary.ReadString(b)
	assert.NoError(t, err)
	b = b[y:]
	fmt.Printf("groupkey: %s, expected: %s\n", expecteds, actuals)
}
