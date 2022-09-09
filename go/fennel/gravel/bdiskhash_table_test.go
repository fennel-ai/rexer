package gravel

import (
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBDiskHashTable(t *testing.T) {
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
		}})
		assert.NoError(t, err)
		idealSize += 8 + 16 + i%50 + 4
	}
	fmt.Println("time_ms insert all data to memtable:", time.Now().Sub(t1).Milliseconds(), "ideal data size", idealSize)

	t1 = time.Now()
	table, err := mt.Flush(BDiskHashTable, "/tmp", 133)
	assert.NoError(t, err)
	fmt.Println("time_ms dump to file:", time.Now().Sub(t1).Milliseconds())

	t1 = time.Now()
	for i := 0; i < itemCnt; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := table.Get(key)
		assert.NoError(t, err)

		if v.expires != 0xABCD1234 {
			panic("bad expire")
		}
		valueNum := int(binary.BigEndian.Uint64(v.data))
		assert.Equal(t, i, valueNum)
		assert.Equal(t, i%50+16, len(v.data))
	}
	fmt.Println("time_ms read from file:", time.Now().Sub(t1).Milliseconds())
}
