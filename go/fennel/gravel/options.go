package gravel

import "runtime"

type TableType uint8

const (
	//  WARNING: existing table type value should never be changed
	testTable    TableType = 0 // a table type used only for tests
	HashTable    TableType = 3
	InvalidTable TableType = 4 // marker for the last table type, increment if new type is inserted above
)

type Options struct {
	MaxMemtableSize     uint64
	TableType           TableType
	Dirname             string
	Name                string
	ReportStats         bool
	NumShards           uint64
	CompactionWorkerNum int
}

func DefaultOptions() Options {
	compactionWorkerNum := runtime.NumCPU() / 5
	if compactionWorkerNum > 8 {
		compactionWorkerNum = 8
	}
	if compactionWorkerNum < 1 {
		compactionWorkerNum = 1
	}

	return Options{
		MaxMemtableSize:     1 << 30, // 1GB
		TableType:           HashTable,
		Dirname:             "",   // current directory
		Name:                "",   // name of the DB - useful when reading stats from multiple instances
		ReportStats:         true, // should stats be exported to prometheus or not
		NumShards:           4,
		CompactionWorkerNum: compactionWorkerNum,
	}
}

func (o Options) WithDirname(dirname string) Options {
	o.Dirname = dirname
	return o
}

func (o Options) WithMaxTableSize(sz uint64) Options {
	o.MaxMemtableSize = sz
	return o
}

func (o Options) WithName(name string) Options {
	o.Name = name
	return o
}

func (o Options) WithNumShards(shards int) Options {
	o.NumShards = uint64(shards)
	return o
}

func (o Options) WithCompactionWorkerNum(workers int) Options {
	o.CompactionWorkerNum = workers
	return o
}
