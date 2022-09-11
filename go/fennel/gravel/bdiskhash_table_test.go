package gravel

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TODO: remove this file and merge any special contents with table_test.go
// which contains tests that apply to all interface implementations
func TestBDiskHashTable(t *testing.T) {
	t.Skip("Test takes too long so skipping it. We do have some coverage in more generate test_table")
	itemCnt := 10_000_000
	mt := NewMemTable()
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
	table, err := mt.Flush(BDiskHashTable, "/tmp", 133)
	assert.NoError(t, err)
	fmt.Println("time_ms dump to file:", time.Since(t1).Milliseconds())

	t1 = time.Now()
	for i := 0; i < itemCnt; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := table.Get(key, Hash(key))
		assert.NoError(t, err)

		if v.expires != 0xABCD1234 {
			panic("bad expire")
		}
		valueNum := int(binary.BigEndian.Uint64(v.data))
		assert.Equal(t, i, valueNum)
		assert.Equal(t, i%50+16, len(v.data))
	}
	fmt.Println("time_ms read from file:", time.Since(t1).Milliseconds())

	t1 = time.Now()
	key = make([]byte, 9)
	// query nonexist records
	for i := 0; i < itemCnt; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		_, err := table.Get(key, Hash(key))
		assert.Equal(t, err, ErrNotFound)
	}
	fmt.Println("time_ms read from file for non-exist records:", time.Since(t1).Milliseconds())
}
