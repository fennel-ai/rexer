package gravel

type TableType uint8

const (
	testTable      TableType = 0 // a table type used only for tests
	BTreeTable     TableType = 1
	BDiskHashTable TableType = 2
)

type Options struct {
	MaxTableSize uint64
	TableType    TableType
	Dirname      string
	Name         string
	ReportStats  bool
	NumShards    uint64
}

func DefaultOptions() Options {
	return Options{
		MaxTableSize: 1 << 30, // 1GB
		TableType:    BDiskHashTable,
		Dirname:      "",   // current directory
		Name:         "",   // name of the DB - useful when reading stats from multiple instances
		ReportStats:  true, // should stats be exported to prometheus or not
		NumShards:    4,
	}
}

func (o Options) WithDirname(dirname string) Options {
	o.Dirname = dirname
	return o
}

func (o Options) WithMaxTableSize(sz uint64) Options {
	o.MaxTableSize = sz
	return o
}
