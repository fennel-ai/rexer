package gravel

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
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

func TestFull(t *testing.T) {
	t.Skip("Test takes too long so skipping it. We do have some coverage in more generate test_table")
	dirname := t.TempDir()
	shards := uint64(256)
	manifest, err := InitManifest(dirname, HashTable, shards)

	itemCnt := 10_000_000
	mt := NewMemTable(shards)
	key := make([]byte, 8)
	t1 := time.Now()
	idealSize := 0

	for i := 0; i < itemCnt; i++ {
		value := make([]byte, 16+i%50)
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(value, uint64(i))
		binary.BigEndian.PutUint64(value[8:], uint64(i))
		err := mt.SetMany([]Entry{{
			key: key,
			val: Value{value, 0xABCD1234, false},
		}}, &Stats{})
		assert.NoError(t, err)
		idealSize += 8 + 16 + i%50 + 4
	}
	fmt.Println("time_ms insert all data to memtable:", time.Since(t1).Milliseconds(), "ideal data size", idealSize)

	t1 = time.Now()
	tmpTableFiles, err := mt.Flush(HashTable, dirname)
	err = manifest.Append(tmpTableFiles)
	assert.NoError(t, err)
	fmt.Println("time_ms dump to file:", time.Since(t1).Milliseconds())

	t1 = time.Now()
	for i := 0; i < itemCnt; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))

		shardHash := ShardHash(key)
		shard := shardHash & (manifest.numShards - 1)
		tables, err := manifest.List(shard)
		assert.NoError(t, err)
		v, err := tables[0].Get(key, Hash(key))

		assert.NoError(t, err)
		if v.expires != 0xABCD1234 {
			panic("bad expire")
		}
		valueNum := int(binary.BigEndian.Uint64(v.data))
		assert.Equal(t, i, valueNum)
		assert.Equal(t, i%50+16, len(v.data))
	}
	fmt.Println("time_ms read from file:", time.Since(t1).Milliseconds())

	key = make([]byte, 9)

	// query nonexist records, for 3 times
	var keys [][]byte
	for i := 0; i < itemCnt; i++ {
		curKey := make([]byte, 9)
		binary.BigEndian.PutUint64(curKey, uint64(rand.Int()))
		keys = append(keys, curKey)
	}

	t1 = time.Now()
	for _, curKey := range keys {
		shardHash := ShardHash(curKey)
		shard := shardHash & (manifest.numShards - 1)
		tables, err := manifest.List(shard)
		assert.NoError(t, err)
		_, err = tables[0].Get(curKey, Hash(curKey))
		assert.Equal(t, err, ErrNotFound)
	}
	fmt.Println("time_ms read from file for non-exist records:", time.Since(t1).Milliseconds())

	keys = nil
	for i := 0; i < itemCnt; i++ {
		curKey := make([]byte, 9)
		binary.BigEndian.PutUint64(curKey, uint64(rand.Int()))
		keys = append(keys, curKey)
	}

	t1 = time.Now()
	for _, curKey := range keys {
		shardHash := ShardHash(curKey)
		shard := shardHash & (manifest.numShards - 1)
		tables, err := manifest.List(shard)
		assert.NoError(t, err)
		_, err = tables[0].Get(curKey, Hash(curKey))
		assert.Equal(t, err, ErrNotFound)
	}
	fmt.Println("time_ms read from file for non-exist records:", time.Since(t1).Milliseconds())

	keys = nil
	for i := 0; i < itemCnt; i++ {
		curKey := make([]byte, 9)
		binary.BigEndian.PutUint64(curKey, uint64(rand.Int()))
		keys = append(keys, curKey)
	}

	t1 = time.Now()
	for _, curKey := range keys {
		shardHash := ShardHash(curKey)
		shard := shardHash & (manifest.numShards - 1)
		tables, err := manifest.List(shard)
		assert.NoError(t, err)
		_, err = tables[0].Get(curKey, Hash(curKey))
		assert.Equal(t, err, ErrNotFound)
	}
	fmt.Println("time_ms read from file for non-exist records:", time.Since(t1).Milliseconds())
}
