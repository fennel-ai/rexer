package gravel

import (
	"fennel/lib/utils"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHashTable(t *testing.T) {
	testTableType(t, HashTable, 1000_000)
}

func BenchmarkHashTable(b *testing.B) {
	b.Run("keys_present", func(b *testing.B) {
		benchmarkTableGet(b, 100_1000, HashTable)
	})
	b.Run("keys_absent", func(b *testing.B) {
		benchmarkTableAbsent(b, 100_1000, HashTable)
	})
}

func testTableType(t *testing.T, type_ TableType, sz int) {
	rand.Seed(time.Now().Unix())
	numShards := 4
	mt := getMemTable(sz, numShards)
	id := rand.Uint64()
	dirname := fmt.Sprintf("/tmp/gravel-%d", id)
	start := time.Now()
	filenames, err := BuildTable(dirname, uint64(numShards), type_, &mt)
	tables := make([]Table, numShards)
	for i, fname := range filenames {
		newname := fmt.Sprintf("%d_%d%s", i, 1, FileExtension)
		newpath := path.Join(dirname, newname)
		err = os.Rename(path.Join(dirname, fname), newpath)
		assert.NoError(t, err)
		tables[i], err = OpenTable(type_, newpath)
		assert.NoError(t, err)
	}
	assert.NoError(t, err)
	duration := time.Since(start)
	fmt.Printf("Table build took: %f second\n", duration.Seconds())
	defer func() {
		err := os.RemoveAll(dirname)
		if err != nil {
			panic(err)
		}
	}()
	presentTime := int64(0)
	absentTime := int64(0)
	for s := 0; s < numShards; s++ {
		for k, v := range mt.Iter(uint64(s)) {
			start := time.Now()
			got, err := tables[s].Get([]byte(k), Hash([]byte(k)))
			presentTime += time.Since(start).Nanoseconds()
			assert.NoError(t, err, fmt.Sprintf("key: %s not found", k))
			assert.Equal(t, v, got)
		}
		for i := 0; i < int(mt.Len()); i++ {
			k := []byte(utils.RandString(10))
			start := time.Now()
			_, err := tables[s].Get(k, Hash([]byte(k)))
			absentTime += time.Since(start).Nanoseconds()
			assert.Error(t, err)
			assert.Equal(t, ErrNotFound, err)
		}
	}
	fmt.Printf("Time taken to make all gets for present keys: %dms\n", presentTime/1e6)
	fmt.Printf("Time taken to make all gets for absent keys: %dms\n", absentTime/1e6)
}

func benchmarkTableGet(b *testing.B, sz int, type_ TableType) {
	numShards := 4
	mt := getMemTable(sz, numShards)
	id := rand.Uint64()
	dirname := fmt.Sprintf("/tmp/gravel-%d", id)
	filenames, _ := BuildTable(dirname, uint64(numShards), type_, &mt)
	tables := make([]Table, numShards)
	for i, fname := range filenames {
		newname := fmt.Sprintf("%d_%d%s", i, 1, FileExtension)
		newpath := path.Join(dirname, newname)
		err := os.Rename(path.Join(dirname, fname), newpath)
		if err != nil {
			panic(err)
		}
		tables[i], _ = OpenTable(type_, newpath)
	}
	defer func() {
		err := os.RemoveAll(dirname)
		if err != nil {
			panic(err)
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	var got Value
	for iter := 0; iter < b.N; iter++ {
		for s := 0; s < numShards; s++ {
			for k := range mt.Iter(uint64(s)) {
				h := Hash([]byte(k))
				b.StartTimer()
				got, _ = tables[s].Get([]byte(k), h)
				b.StopTimer()
			}
		}
	}
	fmt.Printf("dummy got was: %v\n", got)
}

func benchmarkTableAbsent(b *testing.B, sz int, type_ TableType) {
	numShards := 4
	mt := getMemTable(sz, numShards)
	id := rand.Uint64()
	dirname := fmt.Sprintf("/tmp/gravel-%d", id)
	filenames, _ := BuildTable(dirname, uint64(numShards), type_, &mt)
	tables := make([]Table, numShards)
	for i, fname := range filenames {
		newname := fmt.Sprintf("%d_%d%s", i, 1, FileExtension)
		newpath := path.Join(dirname, newname)
		err := os.Rename(path.Join(dirname, fname), newpath)
		if err != nil {
			panic(err)
		}
		tables[i], _ = OpenTable(type_, newpath)
	}
	defer func() {
		err := os.RemoveAll(dirname)
		if err != nil {
			panic(err)
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	var got Value
	for iter := 0; iter < b.N; iter++ {
		for s := 0; s < numShards; s++ {
			for k := range mt.Iter(uint64(s)) {
				h := Hash([]byte(k))
				q := (s + 1) % numShards // query it from the wrong shard so that each key is a miss
				b.StartTimer()
				got, _ = tables[q].Get([]byte(k), h)
				b.StopTimer()
			}
		}
	}
	fmt.Printf("dummy got was: %v\n", got)
}

func getMemTable(sz, numShards int) Memtable {
	mt := NewMemTable(uint64(numShards))
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
