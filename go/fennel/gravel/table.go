package gravel

import (
	"context"
	"errors"
	"fennel/lib/timer"
	"fennel/lib/utils"
	"fennel/lib/utils/binary"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

const maxCompactBatch = 4
const tableSizeLimit = 2 * 1024 * 1024 * 1024

type Table interface {
	Name() string
	Get(key []byte, hash uint64) (Value, error)
	GetAll(m map[string]Value) error
	Size() uint64
	NumRecords() uint64
	Close() error
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

	filenames := make([]string, 0, numShards)
	for shard := uint64(0); shard < numShards; shard += 1 {
		data := mt.Iter(shard)
		var filename string
		if len(data) == 0 {
			filename = ""
		} else {
			filename = fmt.Sprintf("%d_%d%s", shard, time.Now().UnixMicro(), tempFileExtension)
			filepath := path.Join(dirname, filename)
			var err error = nil
			switch type_ {
			/*
				case BTreeTable:
					return buildBTreeTable(dirname, numShards, mt)
				case BDiskHashTable:
					return buildBDiskHashTable(dirname, numShards, mt)
				case testTable:
					return buildEmptyTable(dirname, numShards, mt)

			*/
			case HashTable:
				err = buildHashTable(filepath, data)
			default:
				err = fmt.Errorf("invalid table type")
			}
			if err != nil {
				return nil, err
			}
		}
		filenames = append(filenames, filename)
	}
	return filenames, nil
}

func PickTablesToCompact(tables []Table) []Table {
	// merge strategy:
	// 1. Merge as many consecutive files as possible, but less than maxCompactionBatch and total size less than tableSizeLimit
	// 2. If there are multiple choices, choose the one with the smallest total size
	type entry struct {
		startIdx  int
		totalSize uint64
		fileCnt   int
	}

	var entries []entry = nil
	for idx, table := range tables {
		entry := entry{
			startIdx:  idx,
			fileCnt:   1,
			totalSize: table.Size(),
		}
		for i := 1; i < maxCompactBatch; i++ {
			if idx+i >= len(tables) {
				break
			}
			entry.totalSize += tables[idx+i].Size()
			entry.fileCnt++
			if entry.totalSize >= tableSizeLimit {
				break
			}
			entries = append(entries, entry)
		}
	}

	if entries == nil {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		ei, ej := entries[i], entries[j]
		return ei.fileCnt > ej.fileCnt || (ei.fileCnt == ej.fileCnt && ei.totalSize < ej.totalSize)
	})

	return tables[entries[0].startIdx : entries[0].startIdx+entries[0].fileCnt]
}

// TODO goroutine(s) check for priorities, pick tables, compact tables, replace file

// CompactTables compact several opened tables into a new temp file,
// tables slice should strictly follow the rule that newer table comes later
// if compacting to the final(oldest) file in the shard, deletion markers will be removed
func CompactTables(dirname string, tables []Table, shardId uint64, type_ TableType, compactToFinal bool) (string, error) {
	filename := fmt.Sprintf("%d_%s%s", shardId, utils.RandString(8), tempFileExtension)
	filepath := path.Join(dirname, filename)

	var err error = nil
	m := make(map[string]Value)
	for _, table := range tables {
		err = table.GetAll(m)
		if err != nil {
			return "", err
		}
	}
	if compactToFinal {
		// remove expired items and deletion marker in the furthest file
		now := Timestamp(time.Now().Unix())
		for k, v := range m {
			if v.deleted || (v.expires > 0 && v.expires < now) {
				delete(m, k)
			}
		}
	}
	switch type_ {
	/*
		case BDiskHashTable:
			return buildBDiskHashTable(dirname, numShards, mt)
		case testTable:
			return buildEmptyTable(dirname, numShards, mt)
	*/
	case HashTable:
		err = buildHashTable(filepath, m)
	default:
		err = fmt.Errorf("compaction is not supported for such table type")
	}
	if err != nil {
		return "", err
	}
	return filepath, nil
}

func OpenTable(type_ TableType, filepath string) (Table, error) {
	_, t := timer.Start(context.TODO(), 1, "gravel.table.open")
	defer t.Stop()
	_, fname := path.Split(filepath)
	if !strings.HasSuffix(fname, FileExtension) {
		return nil, errors.New("can not open table - not .grvl file")
	}
	switch type_ {
	case BTreeTable:
		return openBTreeTable(filepath)
	case BDiskHashTable:
		return openBDiskHashTable(filepath)
	case testTable:
		return openEmptyTable()
	case HashTable:
		return openHashTable(filepath, true, false)
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
