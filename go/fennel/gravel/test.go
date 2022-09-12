package gravel

import (
	"fennel/lib/utils"
	"fmt"
	"os"
	"path"
)

type emptyTable struct {
	id uint64
}

func (d emptyTable) Get(key []byte, hash uint64) (Value, error) {
	return Value{}, fmt.Errorf("method not defined")
}

func (d emptyTable) Close() error {
	return nil
}

func (d emptyTable) ID() uint64 {
	return d.id
}

func (d emptyTable) DataReads() uint64 {
	return 0
}

var _ Table = (*emptyTable)(nil)

func openEmptyTable(id uint64) (Table, error) {
	return emptyTable{id: id}, nil
}
func buildEmptyTable(dirname string, numShards uint64, _ *Memtable) ([]string, error) {
	filenames := make([]string, 0)
	for i := 0; i < int(numShards); i++ {
		fname := fmt.Sprintf("%d_%s%s", i, utils.RandString(5), tempSuffix)
		filenames = append(filenames, fname)
		f, err := os.Create(path.Join(dirname, fname))
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
	}
	return filenames, nil
}
