package gravel

import (
	"errors"
	"fennel/lib/utils/binary"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
)

type Table interface {
	Get(key []byte) (Value, error)
	Close() error
	ID() uint64
}

// BuildTable persists a memtable on disk and returns a Table
// that has a readonly handle to the table
func BuildTable(dirname string, id uint64, type_ TableType, mt *Memtable) (Table, error) {
	// before opening the file, first make sure the directory exists
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		return nil, err
	}
	switch type_ {
	case BTreeTable:
		return buildBTreeTable(dirname, id, mt)
	case BBHashTable:
		return buildBBHashTable(dirname, id, mt)
	case BDiskHashTable:
		return buildBDiskHashTable(dirname, id, mt)
	default:
		return nil, fmt.Errorf("invalid table type")
	}
}

func OpenTable(type_ TableType, filepath string) (Table, error) {
	_, fname := path.Split(filepath)
	if !strings.HasSuffix(fname, SUFFIX) {
		return nil, errors.New("can not open table - not .grvl file")
	}
	end := len(fname) - len(SUFFIX)
	id, err := strconv.ParseUint(fname[:end], 10, 64)
	if err != nil {
		return nil, err
	}
	switch type_ {
	case BTreeTable:
		return openBTreeTable(id, filepath)
	case BBHashTable:
		return openBBHashTable(id, filepath)
	default:
		return nil, fmt.Errorf("invalid table type: %v", type_)
	}
}

/*
	Encoding scheme of value is as follows:

	1 byte for deletion flag | 4 bytes for expiration | remaining for value

	If the key was deleted (vs set to a value), deletion flag is 1 and the rest of
	entries aren't even included.
*/

func encodeVal(v Value) ([]byte, error) {
	if v.deleted {
		return []byte{1}, nil
	}
	buf := make([]byte, 1+4+len(v.data))
	buf[0] = 0
	k, err := binary.PutUint32(buf[1:], uint32(v.expires))
	if err != nil {
		return nil, err
	}
	if k < 4 {
		return nil, errors.New("could not encode value expiration")
	}
	n := copy(buf[5:], v.data)
	if n < len(v.data) {
		return nil, errors.New("unable to encode value data")
	}
	return buf[:n+5], nil
}

func decodeVal(data []byte) (Value, error) {
	if len(data) == 0 {
		return Value{}, errors.New("buffer too small for decoding the value")
	}
	deleted := data[0] == 1
	if deleted {
		return Value{deleted: true}, nil
	}
	expires, n, err := binary.ReadUint32(data[1:])
	if err != nil {
		return Value{}, errors.New("error in decoding expiry")
	}
	if n < 4 {
		return Value{}, errors.New("error in decoding expiry")
	}
	return Value{
		data:    data[5:],
		expires: Timestamp(expires),
		deleted: false,
	}, nil
}

func sizeof(e Entry) int {
	sz := len(e.key) + 1
	if !e.val.deleted {
		sz += len(e.val.data) + 4
	}
	return sz
}
