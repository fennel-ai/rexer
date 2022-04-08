package fbadger

import (
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/resource"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
)

const (
	tempdir = "/tmp/"
)

func newDB(memory bool) (DB, error) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	scope := resource.NewTierScope(tierID)

	var opts badger.Options
	if !memory {
		dir, err := ioutil.TempDir(tempdir, fmt.Sprintf("badger-%d", tierID))
		if err != nil {
			return DB{}, err
		}
		opts = badger.DefaultOptions(dir)
	} else {
		opts = badger.DefaultOptions("").WithInMemory(true)
	}
	opts = opts.WithLoggingLevel(badger.ERROR)

	conf := Config{
		Opts:  opts,
		Scope: scope,
	}
	db, err := conf.Materialize()
	if err != nil {
		return DB{}, err
	}
	return db.(DB), nil
}

func cleanup(db *DB) error {
	if err := db.Close(); err != nil {
		return err
	}
	if !db.Opts().InMemory {
		return os.RemoveAll(db.Opts().Dir)
	}
	return nil
}

func TestDB(t *testing.T) {
	t.Parallel()
	t.Run("test_badger_basic_disk", func(t *testing.T) {
		db, err := newDB(false)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, cleanup(&db)) }()
		testBasic(t, db)
	})

	t.Run("test_badger_basic_memory", func(t *testing.T) {
		db, err := newDB(true)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, cleanup(&db)) }()
		testBasic(t, db)
	})
	t.Run("test_badger_write_batch_disk", func(t *testing.T) {
		db, err := newDB(false)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, cleanup(&db)) }()
		testWriteBatch(t, db)
	})

	t.Run("test_badger_write_batch_memory", func(t *testing.T) {
		db, err := newDB(true)
		assert.NoError(t, err)
		defer func() { assert.NoError(t, cleanup(&db)) }()
		testWriteBatch(t, db)
	})
}

func testBasic(t *testing.T, db DB) {
	key := []byte("key")
	value1 := []byte("value1")
	value2 := []byte("value1")
	// initially value is not found
	db.View(func(tx *badger.Txn) error {
		_, err := tx.Get(key)
		assert.Equal(t, badger.ErrKeyNotFound, err)
		return err
	})
	// set the value1
	assert.NoError(t, db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value1)
	}))

	// and verify it is found
	db.View(func(tx *badger.Txn) error {
		entry, err := tx.Get(key)
		assert.NoError(t, entry.Value(func(val []byte) error {
			assert.Equal(t, value1, val)
			return nil
		}))
		return err
	})
	// change the value1 to value2
	assert.NoError(t, db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, value2)
	}))

	// and verify it is found
	db.View(func(tx *badger.Txn) error {
		entry, err := tx.Get(key)
		assert.NoError(t, entry.Value(func(val []byte) error {
			assert.Equal(t, value2, val)
			return nil
		}))
		return err
	})

	// delete the value1 and verify it is gone
	db.Update(func(tx *badger.Txn) error {
		err := tx.Delete(key)
		assert.NoError(t, err)
		_, err = tx.Get(key)
		assert.Equal(t, badger.ErrKeyNotFound, err)
		return err
	})
}

func testWriteBatch(t *testing.T, db DB) {
	keys := make([][]byte, 10)
	vals := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte(utils.RandString(10))
		vals[i] = []byte(utils.RandString(10))
	}
	// initially all keys are not found
	db.View(func(tx *badger.Txn) error {
		for i := 0; i < 10; i++ {
			_, err := tx.Get(keys[i])
			assert.Equal(t, badger.ErrKeyNotFound, err)
		}
		return nil
	})
	// set values in a batch
	batch := db.NewWriteBatch()
	for i := range keys {
		batch.Set(keys[i], vals[i])
	}
	assert.NoError(t, batch.Flush())
	db.View(func(tx *badger.Txn) error {
		for i := 0; i < 10; i++ {
			entry, err := tx.Get(keys[i])
			assert.NoError(t, err)
			entry.Value(func(val []byte) error {
				assert.Equal(t, vals[i], val)
				return nil
			})
		}
		return nil
	})
}

func benchmarkWritesReads(b *testing.B, numRows int, keysz, valsz int) {
	keys := make([][]byte, numRows)
	vals := make([][]byte, numRows)
	for i := 0; i < numRows; i++ {
		keys[i] = []byte(utils.RandString(keysz))
		vals[i] = []byte(utils.RandString(valsz))
	}
	b.ResetTimer()
	// benchmark sets all the keys and then reads all the keys
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db, _ := newDB(false)
		b.StartTimer()
		// set values in a batch
		batch := db.NewWriteBatch()
		for i := range keys {
			batch.Set(keys[i], vals[i])
		}
		batch.Flush()
		db.View(func(tx *badger.Txn) error {
			for i := 0; i < numRows; i++ {
				_, _ = tx.Get(keys[i])
			}
			return nil
		})
		b.StopTimer()
		cleanup(&db)
		b.StartTimer()
	}
}

func Benchmark_BatchWritesReads(b *testing.B) {
	b.Run("10Krows__100byte_keys__10byte_values", func(b *testing.B) {
		benchmarkWritesReads(b, 10000, 100, 10)
	})
	b.Run("10Krows__100byte_keys__100byte_values", func(b *testing.B) {
		benchmarkWritesReads(b, 10000, 100, 100)
	})
	b.Run("10Krows__10byte_keys__100byte_values", func(b *testing.B) {
		benchmarkWritesReads(b, 10000, 10, 1000)
	})
}
