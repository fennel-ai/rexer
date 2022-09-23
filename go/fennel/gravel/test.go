package gravel

import (
	"fennel/lib/utils"
	"fmt"
	"os"
	"path"
)

type emptyTable struct {
}

func (d emptyTable) Name() string {
	return "whatever"
}

func (d emptyTable) NumRecords() uint64 {
	return 0
}

func (d emptyTable) Get(key []byte, hash uint64) (Value, error) {
	return Value{}, fmt.Errorf("method not defined")
}

func (d emptyTable) Close() error {
	return nil
}

func (d emptyTable) DataReads() uint64 {
	return 0
}

func (d emptyTable) GetAll(_ map[string]Value) error {
	return nil
}

func (d emptyTable) Size() uint64 {
	return 0
}

var _ Table = (*emptyTable)(nil)

func openEmptyTable() (Table, error) {
	return emptyTable{}, nil
}

func buildEmptyTable(dirname string, numShards uint64, _ *Memtable) ([]string, error) { // nolint
	filenames := make([]string, 0)
	for i := 0; i < int(numShards); i++ {
		fname := fmt.Sprintf("%d_%s%s", i, utils.RandString(5), tempFileExtension)
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
