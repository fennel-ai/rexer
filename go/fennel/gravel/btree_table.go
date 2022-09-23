package gravel

import (
	"bytes"
	"errors"
	"fennel/lib/utils"
	"fmt"
	"os"
	"path"
	"sort"

	"go.uber.org/zap"

	"go.etcd.io/bbolt"
	"go.uber.org/atomic"
)

// TODO: we have currently disabled bloom filter because it creates problems with race
// test detection. Either turn it back on or simply remove Btreetable from codebase.
// This table without bloom should NEVER be used in prod
type bTreeTable struct {
	db *bbolt.DB
	// bloom *Bloom
	reads atomic.Uint64
}

func (t *bTreeTable) Name() string {
	panic("implement me")
}

func (t *bTreeTable) NumRecords() uint64 {
	panic("implement me")
}

func (t *bTreeTable) DataReads() uint64 {
	return t.reads.Load()
}

func (t *bTreeTable) GetAll(_ map[string]Value) error {
	panic("implement me")
}

func (t *bTreeTable) Size() uint64 {
	panic("implement me")
}

var _ Table = (*bTreeTable)(nil)

const (
	treebucket  = "tree"
	bloombucket = "bloom"
	bloomkey    = "data"
)

func (t *bTreeTable) Get(key []byte, _ uint64) (Value, error) {
	// if !t.bloom.Has(key) {
	// 	return Value{}, ErrNotFound
	// }
	t.reads.Add(1)
	var ret = &Value{}
	err := t.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte(treebucket))
		if b == nil {
			return errors.New("tree bucket is nil")
		}
		v := b.Get(key)
		if v == nil {
			return ErrNotFound
		}
		val, err := decodeVal(v)
		*ret = val
		return err
	})
	return *ret, err
}

func (t *bTreeTable) Close() error {
	return t.db.Close()
}

// TODO: if table creation fails, delete the files before returning
func buildBTreeTable(dirname string, numShards uint64, mt *Memtable) ([]string, error) { // nolint
	filenames := make([]string, numShards)
	for i := 0; i < int(numShards); i++ {
		iter := mt.Iter(uint64(i))
		// filter := NewBloomFilter(uint64(len(iter)), 0.001)
		filename := fmt.Sprintf("%d_%s%s", i, utils.RandString(8), tempFileExtension)
		filepath := path.Join(dirname, filename)
		filenames[i] = filename
		db, err := bbolt.Open(filepath, 0666, nil)
		if err != nil {
			return nil, fmt.Errorf("could not open file during table building: %w", err)
		}
		defer func(db *bbolt.DB) {
			err := db.Close()
			if err != nil {
				zap.L().Error("failed to close db", zap.Error(err))
			}
		}(db)
		batchsz := 50_000
		entries := make([]Entry, 0, batchsz)
		for k, v := range iter {
			// filter.Add([]byte(k))
			entries = append(entries, Entry{key: []byte(k), val: v})
			if len(entries) >= batchsz {
				sort.Slice(entries, func(i, j int) bool {
					return bytes.Compare(entries[i].key, entries[j].key) <= 0
				})
				err = db.Update(func(tx *bbolt.Tx) error {
					bdata, err := tx.CreateBucketIfNotExists([]byte(treebucket))
					if err != nil {
						return fmt.Errorf("create data bucket failed: %s", err)
					}
					for _, e := range entries {
						val, err := encodeVal(e.val)
						if err != nil {
							return fmt.Errorf("could not encode val while building table: %w", err)
						}
						if err = bdata.Put(e.key, val); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					return nil, err
				}
				entries = entries[:0]
			}
		}
		// add all the remaining data not yet flushed along with bloom filter
		err = db.Update(func(tx *bbolt.Tx) error {
			bdata, err := tx.CreateBucketIfNotExists([]byte(treebucket))
			if err != nil {
				return fmt.Errorf("create data bucket failed: %s", err)
			}
			for _, e := range entries {
				val, err := encodeVal(e.val)
				if err != nil {
					return fmt.Errorf("could not encode val while building table: %w", err)
				}
				if err = bdata.Put(e.key, val); err != nil {
					return err
				}
			}
			return nil
			// bfilter, err := tx.CreateBucket([]byte(bloombucket))
			// if err != nil {
			// 	return fmt.Errorf("create bloom bucket failed: %s", err)
			// }
			// return bfilter.Put([]byte(bloomkey), filter.Dump())
		})
		if err != nil {
			return nil, err
		}
		if err := db.Close(); err != nil {
			return nil, err
		}

	}
	return filenames, nil
}

func openBTreeTable(filepath string) (Table, error) {
	_, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("the file doesn't exist")
	}
	db, err := bbolt.Open(filepath, 0666, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("could not open gravel: %w", err)
	}
	// var filter = &Bloom{}
	// err = db.View(func(tx *bbolt.Tx) error { b := tx.Bucket([]byte(bloombucket))
	// 	if b == nil {
	// 		return errors.New("could not open table: bloom bucket is nil")
	// 	}
	// 	v := b.Get([]byte(bloomkey))
	// 	if v == nil {
	// 		return errors.New("table file does not have a stored bloom filter")
	// 	}
	// 	*filter = LoadBloom(v)
	// 	return nil
	// })
	return &bTreeTable{
		db: db,
		// bloom: filter,
	}, nil
}
