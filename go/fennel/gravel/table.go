package gravel

import (
	"context"
	"errors"
	"fennel/lib/timer"
	"fennel/lib/utils/binary"
	"fmt"
	"os"
	"path"
	"strings"
)

type Table interface {
	Get(key []byte, hash uint64) (Value, error)
	Close() error
	ID() uint64
	DataReads() uint64
}

// BuildTable persists a memtable on disk broken into numShards shards and returns list of
// filenames for each of the shards in the correct order
func BuildTable(dirname string, numShards uint64, type_ TableType, mt *Memtable) ([]string, error) {
	_, t := timer.Start(context.TODO(), 1, "gravel.table.build")
	defer t.Stop()
	// if the directory doesn't exist, create it
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		return nil, err
	}
	switch type_ {
	case BTreeTable:
		return buildBTreeTable(dirname, numShards, mt)
	case BDiskHashTable:
		return buildBDiskHashTable(dirname, numShards, mt)
	case testTable:
		return buildEmptyTable(dirname, numShards, mt)
	case HashTable:
		return buildHashTable(dirname, numShards, mt)
	default:
		return nil, fmt.Errorf("invalid table type")
	}
}

func OpenTable(type_ TableType, id uint64, filepath string) (Table, error) {
	_, t := timer.Start(context.TODO(), 1, "gravel.table.open")
	defer t.Stop()
	_, fname := path.Split(filepath)
	if !strings.HasSuffix(fname, SUFFIX) {
		return nil, errors.New("can not open table - not .grvl file")
	}
	switch type_ {
	case BTreeTable:
		return openBTreeTable(id, filepath)
	case BDiskHashTable:
		return openBDiskHashTable(id, filepath)
	case testTable:
		return openEmptyTable(id)
	case HashTable:
		return openHashTable(id, filepath, true, false)
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
		return Value{deleted: true, data: make([]byte, 0)}, nil
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
