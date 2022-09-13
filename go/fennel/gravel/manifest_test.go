package gravel

import (
	"fennel/lib/utils"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestManifest(t *testing.T) {
	rand.Seed(time.Now().Unix())
	dirname := t.TempDir()
	m, err := InitManifest(dirname, testTable, uint64(16))
	assert.NoError(t, err)
	sz := 10
	filenames := make([][]string, sz)
	// now append a bunch of files
	for round := 0; round < sz; round += 1 {
		this := make([]string, 0, 16)
		for shard := 0; shard < 16; shard++ {
			filename := fmt.Sprintf("%d_%s%s", shard, utils.RandString(8), tempSuffix)
			this = append(this, filename)
		}
		// this should fail because none of the files exist
		assert.Error(t, m.Append(this))
		// but will succeed after files have been created
		for _, filename := range this {
			fullpath := path.Join(dirname, filename)
			f, err := os.Create(fullpath)
			assert.NoError(t, err)
			f.Close()
		}
		assert.NoError(t, m.Append(this))
		filenames[round] = this
	}

	// now verify that tables have appropriate files/IDs
	for i := 0; i < 16; i++ {
		maxsofar := uint64(math.MaxUint64)
		m.Reserve()
		tables, err := m.List(uint64(i))
		assert.NoError(t, err)
		assert.Len(t, tables, sz)
		for _, table := range tables {
			id := table.ID()
			assert.True(t, id < maxsofar)
			maxsofar = id
		}
		m.Release()
	}
}

func TestEmptyInit(t *testing.T) {
	for i := 0; i < 1024; i++ {
		dirname := fmt.Sprintf("%s/%s", os.TempDir(), utils.RandString(5))
		m, err := InitManifest(dirname, BDiskHashTable, uint64(i))
		if i == 0 || i > 512 || (i&(i-1)) > 0 {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, uint64(i), m.numShards)
			assert.Equal(t, dirname, m.dirname)
			assert.Equal(t, BDiskHashTable, m.tableType)
			for s := 0; s < i; s++ {
				m.Reserve()
				tables, err := m.List(uint64(s))
				assert.NoError(t, err)
				assert.Empty(t, tables)
				m.Release()
			}
		}
	}
}
