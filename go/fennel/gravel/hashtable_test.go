package gravel

import (
	"bufio"
	"bytes"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
)

func TestHeader(t *testing.T) {
	head := header{
		magic:       magicHeader,
		codec:       1,
		encrypted:   false,
		compression: 3,
		shardbits:   11,
		numRecords:  882318234,
		numBuckets:  231212,
		datasize:    85724290131234,
		indexsize:   5329710,
		minExpiry:   25234,
		maxExpiry:   823042,
	}
	head.moduloMask = uint64(head.numBuckets) - 1
	assert.Equal(t, 64, int(unsafe.Sizeof(head)))
	var buf bytes.Buffer
	writer := bufio.NewWriterSize(&buf, 1024)
	assert.NoError(t, writeHeader(writer, head))
	writer.Flush()
	bits := buf.Bytes()
	assert.Equal(t, 64, len(bits))

	found, err := readHeader(buf.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, head, found)
}
