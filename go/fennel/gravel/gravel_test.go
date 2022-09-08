package gravel

import (
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
	defer g.Teardown()

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
		defer g.Teardown()

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
