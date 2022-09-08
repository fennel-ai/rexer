package gravel

type TableType uint8

const (
	BTreeTable  TableType = 1
	BBHashTable TableType = 2
)

type Options struct {
	MaxTableSize uint64
	TableType    TableType
	Dirname      string
}

func DefaultOptions() Options {
	return Options{
		MaxTableSize: 1 << 30, // 2GB
		TableType:    BBHashTable,
		Dirname:      "", // current directory
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
