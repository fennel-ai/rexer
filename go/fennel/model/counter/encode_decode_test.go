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
	n, err = binary.PutUvarint(buf[curr:], uint64(16))
	assert.NoError(t, err)
	curr += n
	fmt.Printf("str: %s\n", base91.StdEncoding.EncodeToString(buf[:curr]))
}

func TestRedisKeyPrefixString(t *testing.T) {
	var codec codex.Codex = 1
	buf := make([]byte, 8+8+8+8)
	curr := 0
	n, err := codec.Write(buf[curr:])
	assert.NoError(t, err)
	curr += n
	n, err = binary.PutUvarint(buf[curr:], uint64(16))
	assert.NoError(t, err)
	curr += n
	n, err = binary.PutString(buf[curr:], "foobar")
	assert.NoError(t, err)
	curr += n
	fmt.Printf("str: %s\n", base91.StdEncoding.EncodeToString(buf[:curr]))
}
