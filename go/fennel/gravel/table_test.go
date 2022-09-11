package gravel

import (
	"fennel/lib/utils"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBtreeTable(t *testing.T) {
	testTable(t, BTreeTable, 100_000)
}

func TestDiskHashTable(t *testing.T) {
	testTable(t, BDiskHashTable, 1000_000)
}

func testTable(t *testing.T, type_ TableType, sz int) {
	rand.Seed(time.Now().Unix())
	mt := getMemTable(sz)
	id := rand.Uint64()
	dirname := fmt.Sprintf("/tmp/gravel-%d", id)
	start := time.Now()
	table, err := BuildTable(dirname, id, type_, &mt)
	assert.NoError(t, err)
	duration := time.Since(start)
	fmt.Printf("Table build took: %f seconds", duration.Seconds())
	defer func() { os.RemoveAll(dirname) }()
	for k, v := range mt.Iter() {
		got, err := table.Get([]byte(k), Hash([]byte(k)))
		assert.NoError(t, err, fmt.Sprintf("key: %s not found", k))
		assert.Equal(t, v, got)
	}
	for i := 0; i < 1000; i++ {
		k := []byte(utils.RandString(10))
		_, err := table.Get(k, Hash([]byte(k)))
		assert.Error(t, err)
		assert.Equal(t, ErrNotFound, err)
	}
}

func getMemTable(sz int) Memtable {
	mt := NewMemTable()
	keys := make([][]byte, 0, sz)
	vals := make([][]byte, 0, sz)
	entries := make([]Entry, 0, sz)
	for i := 0; i < sz; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%d", i)))
		vals = append(vals, []byte(fmt.Sprintf("val-%d", i)))
		var v Value
		if i%100 == 0 {
			v.deleted = true
			v.data = make([]byte, 0)
		} else {
			v.data = vals[i]
			v.expires = 0
		}
		entries = append(entries, Entry{key: keys[i], val: v})
	}

	// add all to Memtable before returning
	err := mt.SetMany(entries, &Stats{})
	if err != nil {
		panic(err)
	}
	return mt
}
