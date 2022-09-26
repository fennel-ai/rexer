package gravel

import (
	"bufio"
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHeader(t *testing.T) {
	head := header{
		magic:       magicHeader,
		codec:       1,
		encrypted:   false,
		compression: 3,
		numRecords:  882318234,
		numBuckets:  231212,
		datasize:    85724290131234,
		indexsize:   5329710,
		minExpiry:   25234,
		maxExpiry:   823042,
	}
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
