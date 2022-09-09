package gravel

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"

	"go.etcd.io/bbolt"
)

// TODO: we have currently disabled bloom filter because it creates problems with race
// test detection. Either turn it back on or simply remove Btreetable from codebase.
// This table without bloom should NEVER be used in prod
type bTreeTable struct {
	db *bbolt.DB
	// bloom *Bloom
	id uint64
}

func (t *bTreeTable) ID() uint64 {
	return t.id
}

var _ Table = (*bTreeTable)(nil)

const (
	treebucket  = "tree"
	bloombucket = "bloom"
	bloomkey    = "data"
)

func (t *bTreeTable) Get(key []byte) (Value, error) {
	// if !t.bloom.Has(key) {
	// 	return Value{}, ErrNotFound
	// }
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
func buildBTreeTable(dirname string, id uint64, mt *Memtable) (Table, error) {
	iter := mt.Iter()
	// filter := NewBloomFilter(uint64(len(iter)), 0.001)
	filepath := path.Join(dirname, fmt.Sprintf("%d%s", id, SUFFIX))
	fmt.Printf("file path is: %s\n", filepath)
	db, err := bbolt.Open(filepath, 0666, nil)
	if err != nil {
		return nil, fmt.Errorf("could not open file during table building: %w", err)
	}
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
	db.Close()
	// now open the table in just readonly mode and return that
	return openBTreeTable(id, filepath)
}

func openBTreeTable(id uint64, filepath string) (Table, error) {
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
	if err != nil {
		return nil, err
	}
	return &bTreeTable{
		db: db,
		// bloom: filter,
		id: id,
	}, nil
}
