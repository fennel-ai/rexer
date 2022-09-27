package gravel

import (
	"encoding/binary"
	"fennel/lib/utils"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGravel(t *testing.T) {
	rand.Seed(time.Now().Unix())
	dirname := fmt.Sprintf("/tmp/gravel-%d", rand.Uint32())
	g, err := Open(DefaultOptions().WithDirname(dirname).WithMaxTableSize(1000))
	assert.NoError(t, err)
	defer func() {
		if err := g.Teardown(); err != nil {
			panic(err)
		}
	}()

	var keys, vals [][]byte
	for i := 0; i < 100; i++ {
		keys = append(keys, []byte(fmt.Sprintf("%s-key-%d", utils.RandString(10), i)))
		vals = append(vals, []byte(fmt.Sprintf("%s-val-%d", utils.RandString(10), i)))
		_, err := g.Get(keys[i])
		assert.Equal(t, ErrNotFound, err)
	}
	for i := 0; i < 100; i += 10 {
		func() {
			b := g.NewBatch()
			defer b.Discard()
			for j := i; j < i+10; j++ {
				assert.NoError(t, b.Set(keys[j], vals[j], 0))
			}
			assert.NoError(t, b.Commit())
		}()
	}
	// now all the keys should exist
	for i, k := range keys {
		got, err := g.Get(k)
		assert.NoError(t, err)
		assert.Equal(t, got, vals[i])
	}
}

func TestGravelTooLargeBatch(t *testing.T) {
	// TODO: make this better - check that exactly upto the limit works, else it doesn't
	canWrite := func(batch int) error {
		rand.Seed(time.Now().Unix())
		dirname := fmt.Sprintf("/tmp/gravel-%d", rand.Uint32())
		g, err := Open(DefaultOptions().WithDirname(dirname).WithMaxTableSize(1000))
		assert.NoError(t, err)
		defer func() {
			if err := g.Teardown(); err != nil {
				panic(err)
			}
		}()

		var keys, vals [][]byte
		for i := 0; i < batch; i++ {
			keys = append(keys, []byte(fmt.Sprintf("%s-key-%d", utils.RandString(10), i)))
			vals = append(vals, []byte(fmt.Sprintf("%s-val-%d", utils.RandString(10), i)))
		}
		b := g.NewBatch()
		for i, k := range keys {
			assert.NoError(t, b.Set(k, vals[i], 0))
		}
		return b.Commit()
	}
	// if the total size of batch is small, we can commit
	assert.NoError(t, canWrite(10))
	// else we can not commit
	assert.Error(t, canWrite(100))

}

func TestFull(t *testing.T) {
	//t.Skip("Skipping test in pull request since it more or less depends on the performance of the running environment")
	dirname := t.TempDir()
	heavyTest := true

	var itemCnt int
	compactionWaitSecs := 25
	opt := DefaultOptions()
	opt.Dirname = dirname
	if heavyTest {
		opt.MaxMemtableSize = 1024 * 1024 * 10
		itemCnt = 10_000_000
		opt.NumShards = 16
	} else {
		opt.MaxMemtableSize = 1024 * 1024
		itemCnt = 1_000_000
		opt.NumShards = 2
	}

	g, err := Open(opt)
	assert.NoError(t, err)

	batchSize := 1000
	key := make([]byte, 8)

	t1 := time.Now()
	b := g.NewBatch()
	for i := 0; i < itemCnt; i++ {
		value := make([]byte, 16+i%50)
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(value, uint64(i))
		binary.BigEndian.PutUint64(value[8:], uint64(i))
		err := b.Set(key, value, 0)
		assert.NoError(t, err)
		if len(b.Entries()) > batchSize {
			err = b.Commit()
			assert.NoError(t, err)
		}
	}
	if len(b.Entries()) > 0 {
		err = b.Commit()
		assert.NoError(t, err)
	}
	fmt.Println("time_ms insert all data to DB:", time.Since(t1).Milliseconds())

	if heavyTest {
		fmt.Println("sleeping 10 secs, wait for compaction work to start")
		time.Sleep(10 * time.Second) // wait for compaction
	}

	fmt.Println("overwrite all records with different values and expires")
	t1 = time.Now()
	b = g.NewBatch()
	for i := 0; i < itemCnt; i++ {
		expires := uint32(0)
		if i%5 == 0 {
			expires = uint32(time.Now().Unix()) + 10
		}
		value := make([]byte, 16+i%50)
		binary.BigEndian.PutUint64(key, uint64(i))
		binary.BigEndian.PutUint64(value, uint64(i*2))
		binary.BigEndian.PutUint64(value[8:], uint64(i*2))
		err := b.Set(key, value, expires)
		assert.NoError(t, err)
		if len(b.Entries()) > batchSize {
			err = b.Commit()
			assert.NoError(t, err)
		}
	}
	if len(b.Entries()) > 0 {
		err = b.Commit()
		assert.NoError(t, err)
	}
	fmt.Println("time_ms insert all data to DB:", time.Since(t1).Milliseconds())

	fmt.Printf("sleeping another %d secs, wait for more compaction work to be done\n", compactionWaitSecs)
	time.Sleep(time.Duration(compactionWaitSecs) * time.Second) // wait for compaction

	assert.Less(t, g.tm.GetStats()[StatsNumTables], opt.NumShards*minimumFilesToTriggerCompaction+1, "too many tables, seems the compaction doesn't work")

	t1 = time.Now()
	for i := 0; i < itemCnt; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		v, err := g.Get(key)
		if i%5 == 0 {
			assert.Error(t, ErrNotFound, err)
		} else {
			assert.NoError(t, err)
			valueNum := int(binary.BigEndian.Uint64(v))
			assert.Equal(t, i*2, valueNum)
			assert.Equal(t, i%50+16, len(v))
		}

	}
	fmt.Println("time_ms read from file:", time.Since(t1).Milliseconds())

	// query nonexist records
	var keys [][]byte
	for i := 0; i < itemCnt; i++ {
		curKey := make([]byte, 9)
		binary.BigEndian.PutUint64(curKey, uint64(rand.Int()))
		keys = append(keys, curKey)
	}

	t1 = time.Now()
	for _, curKey := range keys {
		_, err := g.Get(curKey)
		assert.Error(t, ErrNotFound, err)
	}
	fmt.Println("time_ms read from file for non-exist records:", time.Since(t1).Milliseconds())
}
