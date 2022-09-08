package gravel

import (
	"fmt"
	"path"

	"github.com/opencoff/go-bbhash"
)

type bbHashTable struct {
	db    *bbhash.DBReader
	Bloom *Bloom
	id    uint64
}

func (b *bbHashTable) Get(key []byte) (Value, error) {
	buf, err := b.db.Find(key)
	switch err {
	case bbhash.ErrNoKey:
		return Value{}, ErrNotFound
	case nil:
		return decodeVal(buf)
	default:
		return Value{}, err
	}
}

func (b *bbHashTable) Close() error {
	_ = b.db.Close
	return nil
}

func (b *bbHashTable) ID() uint64 {
	return b.id
}

func buildBBHashTable(dirname string, id uint64, mt *Memtable) (Table, error) {
	fmt.Printf("starting to build the table...\n")
	filepath := path.Join(dirname, fmt.Sprintf("%d%s", id, SUFFIX))
	wr, err := bbhash.NewDBWriter(filepath)
	if err != nil {
		return nil, err
	}
	fmt.Printf("opened a db writer...\n")
	iter := mt.Iter()
	keys := make([][]byte, 0, len(iter)+1)
	vals := make([][]byte, 0, len(iter)+1)
	filter := NewBloomFilter(uint64(len(iter)), 0.001)
	for k, v := range iter {
		filter.Add([]byte(k))
		keys = append(keys, []byte(k))
		val, err := encodeVal(v)
		if err != nil {
			return nil, fmt.Errorf("unable to encode value: %w", err)
		}
		vals = append(vals, val)
	}
	// also add the bloom filter in the file
	keys = append(keys, []byte(fmt.Sprintf("__gravel@@%s%s__", bloombucket, bloomkey)))
	vals = append(vals, filter.Dump())
	if _, err = wr.AddKeyVals(keys, vals); err != nil {
		return nil, fmt.Errorf("unable to add key/value pairs to bbhash db: %w", err)
	}
	fmt.Printf("added final batch...\n")

	// Now, freeze the DB and write to disk. We will use a larger "gamma" value to increase
	// chances of finding a minimal perfect hash function.
	err = wr.Freeze(4.0)
	if err != nil {
		return nil, fmt.Errorf("unable to freeze hash table: %w", err)
	}
	return openBBHashTable(id, filepath)
}

func openBBHashTable(id uint64, filepath string) (Table, error) {
	rd, err := bbhash.NewDBReader(filepath, 128)
	if err != nil {
		return nil, fmt.Errorf("unable to open bbhash db reader: %w", err)
	}
	filterSer, err := rd.Find([]byte(fmt.Sprintf("__gravel@@%s%s__", bloombucket, bloomkey)))
	if err != nil {
		return nil, fmt.Errorf("unable to load bloom filter from disk: %w", err)
	}
	filter := LoadBloom(filterSer)
	return &bbHashTable{
		db:    rd,
		Bloom: &filter,
		id:    id,
	}, nil
}

var _ Table = (*bbHashTable)(nil)
