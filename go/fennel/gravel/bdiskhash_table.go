package gravel

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"sort"

	"github.com/cespare/xxhash/v2"
	"golang.org/x/exp/mmap"
	"golang.org/x/sys/unix"
)

/*
Table Structure:

64 bytes:
	4 bytes MagicHeader
	4 bytes bucketCount (roughly item_count / 4, tunable)
	4 bytes itemCount
	4 bytes DataPos
	34 bytes TBD

Array Of uint32 (# of elements being 'headSize'),  hash head table
Consecutive hash buckets, each being [(1 or 5 bytes) bucket size,  5 bytes of first record data offset relative to DataPos, bucket_size * 4 bytes hash fingerprint for each item]
Consecutive records of [4 bytes total size, 1 byte key size, 4 bytes value size, 4 bytes expire time, key, value]	// TODO checksum
EOF
*/

const headerSize uint64 = 64
const avgBucketSize uint64 = 12
const invalidUint32 uint32 = 0xFFFFFFFF

type bDiskHashTable struct {
	data        *mmap.ReaderAt
	itemCount   uint64
	bucketCount uint64
	dataPos     uint64
	id          uint64
}

func (b *bDiskHashTable) Get(key []byte) (Value, error) {
	hash := xxhash.Sum64(key)
	bucketId := hash % b.bucketCount

	buf4 := make([]byte, 4)
	_, err := b.data.ReadAt(buf4, int64(headerSize+bucketId*4))
	if err != nil {
		return Value{}, err
	}

	relativeBucketOffset := binary.BigEndian.Uint32(buf4)
	if relativeBucketOffset == invalidUint32 {
		return Value{}, ErrNotFound
	}
	bucketOffset := uint64(relativeBucketOffset) + headerSize + b.bucketCount*4

	itemsInBucket := int(b.data.At(int(bucketOffset)))
	bucketOffset += 1
	if itemsInBucket == 255 {
		_, err := b.data.ReadAt(buf4, int64(bucketOffset))
		if err != nil {
			return Value{}, err
		}
		bucketOffset += 4
		itemsInBucket = int(binary.BigEndian.Uint32(buf4))
	}

	bufCurrBucket := make([]byte, 5+itemsInBucket*4)
	_, err = b.data.ReadAt(bufCurrBucket, int64(bucketOffset))
	if err != nil {
		return Value{}, err
	}

	dataPos := (uint64(bufCurrBucket[0]) << 32) | (uint64(bufCurrBucket[1]) << 24) | (uint64(bufCurrBucket[2]) << 16) | (uint64(bufCurrBucket[3]) << 8) | (uint64(bufCurrBucket[4]))
	hashFP := uint32(hash & 0xFFFFFFFF)

	var matchIndices []int = nil
	for i := 0; i < itemsInBucket; i++ {
		currFP := binary.BigEndian.Uint32(bufCurrBucket[i*4+5:])
		if currFP == hashFP {
			matchIndices = append(matchIndices, i)
		}
	}
	if len(matchIndices) == 0 {
		return Value{}, ErrNotFound
	}

	curDataPos := dataPos + b.dataPos
	matchIdx := 0
	for i := 0; ; i++ {
		if i >= itemsInBucket {
			panic("file is inconsistent state")
		}
		_, err := b.data.ReadAt(buf4, int64(curDataPos))
		if err != nil {
			return Value{}, err
		}
		// write record: [4 bytes total size, 1 byte key size, 4 bytes value size, 1 byte delete tombstone, 4 bytes expire time, key, value]
		recordSize := binary.BigEndian.Uint32(buf4)

		if i == matchIndices[matchIdx] {
			// fmt.Printf("record size is: %d\n", recordSize)
			record := make([]byte, recordSize)
			_, err = b.data.ReadAt(record, int64(curDataPos))
			// fmt.Printf("record is: %v\n", record)
			keyLen := uint64(b.data.At(int(curDataPos + 4)))
			curKey := make([]byte, keyLen)
			_, err = b.data.ReadAt(curKey, int64(curDataPos+4+1+4+1+4))
			if err != nil {
				return Value{}, err
			}
			if bytes.Equal(curKey, key) {
				// found
				buf := make([]byte, 9)
				_, err := b.data.ReadAt(buf, int64(curDataPos+4+1))
				if err != nil {
					return Value{}, err
				}
				valueSize := binary.BigEndian.Uint32(buf)
				delflag := buf[4]
				expTime := binary.BigEndian.Uint32(buf[5:])
				deleted := false
				if delflag > 0 {
					deleted = true
				}
				value := make([]byte, valueSize)
				_, err = b.data.ReadAt(value, int64(curDataPos+4+1+4+1+4+keyLen))
				if err != nil {
					return Value{}, err
				}
				return Value{
					data:    value,
					expires: Timestamp(expTime),
					deleted: deleted,
				}, nil
			}

			// hash fingerprint matched but key didn't match
			matchIdx++
			if matchIdx >= len(matchIndices) {
				break
			}
		}
		curDataPos += uint64(recordSize)
	}
	return Value{}, ErrNotFound
}

func (b *bDiskHashTable) Close() error {
	return b.data.Close()
}

func (b *bDiskHashTable) ID() uint64 {
	return b.id
}

func buildBDiskHashTable(dirname string, id uint64, mt *Memtable) (Table, error) {
	fmt.Printf("starting to build the table...\n")
	filepath := path.Join(dirname, fmt.Sprintf("%d%s", id, SUFFIX))

	f, err := os.Create(filepath)
	if err != nil {
		return nil, err
	}
	type indexObj struct {
		HashFP   uint32
		BucketID uint32
		k        string
		v        Value
	}

	buildFunc := func() error {
		m := mt.Iter()

		itemCount := len(m)
		bucketCount := uint64(itemCount / int(avgBucketSize))
		if bucketCount == 0 {
			bucketCount = 1
		}
		headSlice := make([]byte, bucketCount*4)
		for i := 0; i < int(bucketCount*4); i++ {
			headSlice[i] = 0xFF
		}

		indexObjs := make([]indexObj, len(m))
		idx := 0
		for key, value := range m {
			hash := xxhash.Sum64String(key)
			indexObjs[idx] = indexObj{
				HashFP:   uint32(hash & 0xFFFFFFFF),
				BucketID: uint32(hash % bucketCount),
				k:        key,
				v:        value,
			}
			idx++
		}
		sort.Slice(indexObjs, func(i, j int) bool {
			return indexObjs[i].BucketID < indexObjs[j].BucketID
		})

		var lastBucketId uint32 = 0xFFFFFFFF
		var bucketsBuf []byte = nil

		// calculate data start pos, to reserve the writing file offset
		dataPos := headerSize + bucketCount*4
		for i, indexObjItem := range indexObjs {
			if indexObjItem.BucketID != lastBucketId {
				bucketSize := 1
				for ; i+bucketSize < len(indexObjs); bucketSize++ {
					if indexObjs[i+bucketSize].BucketID != indexObjItem.BucketID {
						break
					}
				}
				if bucketSize < 255 {
					dataPos += 1
				} else {
					dataPos += 5
				}
				dataPos += 5
				dataPos += uint64(bucketSize * 4)
				lastBucketId = indexObjItem.BucketID
			}
		}

		lastBucketId = 0xFFFFFFFF
		_, err := f.Seek(int64(dataPos), unix.SEEK_SET)
		dataWriter := bufio.NewWriterSize(f, 1024*1024)
		if err != nil {
			return err
		}

		var relativeDataPos uint64 = 0
		buf4 := make([]byte, 4)
		for i, indexObjItem := range indexObjs {
			if indexObjItem.BucketID != lastBucketId {
				// flush
				binary.BigEndian.PutUint32(headSlice[indexObjItem.BucketID*4:], uint32(len(bucketsBuf)))
				bucketSize := 1
				for ; i+bucketSize < len(indexObjs); bucketSize++ {
					if indexObjs[i+bucketSize].BucketID != indexObjItem.BucketID {
						break
					}
				}
				if bucketSize < 255 {
					bucketsBuf = append(bucketsBuf, byte(bucketSize))
				} else {
					tempBuf := make([]byte, 5)
					tempBuf[0] = 255
					binary.BigEndian.PutUint32(tempBuf[1:], uint32(bucketSize))
					bucketsBuf = append(bucketsBuf, tempBuf...)
				}
				bucketsBuf = append(bucketsBuf, byte(relativeDataPos>>32), byte(relativeDataPos>>24), byte(relativeDataPos>>16), byte(relativeDataPos>>8), byte(relativeDataPos))
				lastBucketId = indexObjItem.BucketID
			}
			binary.BigEndian.PutUint32(buf4, indexObjItem.HashFP)
			bucketsBuf = append(bucketsBuf, buf4...)
			// write record: [4 bytes total size, 1 byte key size, 4 bytes value size, 1 byte delete tombstone, 4 bytes expire time, key, value]
			recordBuf := make([]byte, 4+1+4+1+4+len(indexObjItem.k)+len(indexObjItem.v.data))
			binary.BigEndian.PutUint32(recordBuf[0:], uint32(len(recordBuf)))
			recordBuf[4] = byte(len(indexObjItem.k))
			binary.BigEndian.PutUint32(recordBuf[5:], uint32(len(indexObjItem.v.data)))
			if indexObjItem.v.deleted {
				recordBuf[9] = 1
			} else {
				recordBuf[9] = 0
			}
			binary.BigEndian.PutUint32(recordBuf[10:], uint32(indexObjItem.v.expires))
			copy(recordBuf[14:], indexObjItem.k)
			copy(recordBuf[14+recordBuf[4]:], indexObjItem.v.data)
			// fmt.Printf("record is: %v\n", recordBuf)
			relativeDataPos += uint64(len(recordBuf))
			_, err = dataWriter.Write(recordBuf)
		}

		fmt.Println("Index size", dataPos)
		if uint64(len(bucketsBuf))+headerSize+bucketCount*4 != dataPos {
			panic("bad assumption")
		}
		err = dataWriter.Flush()
		if err != nil {
			return err
		}

		_, err = f.Seek(0, unix.SEEK_SET)
		if err != nil {
			return err
		}

		headerBuf := make([]byte, headerSize)
		binary.BigEndian.PutUint32(headerBuf[0:], 0x20220101)          // whatever magic header
		binary.BigEndian.PutUint32(headerBuf[4:], uint32(bucketCount)) // whatever magic header
		binary.BigEndian.PutUint32(headerBuf[8:], uint32(itemCount))   // item count
		binary.BigEndian.PutUint32(headerBuf[12:], uint32(dataPos))    // starting of the acutal data
		_, err = f.Write(headerBuf)
		if err != nil {
			return err
		}
		_, err = f.Write(headSlice)
		if err != nil {
			return err
		}

		_, err = f.Write(bucketsBuf)
		if err != nil {
			return err
		}
		return nil
	}

	err = buildFunc()
	if err != nil {
		return nil, err
	}
	err = f.Close()
	if err != nil {
		return nil, err
	}
	return openBDiskHashTable(id, filepath)
}

func openBDiskHashTable(id uint64, filepath string) (Table, error) {
	data, err := mmap.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("unable to open disk hash file: %w", err)
	}
	buf := make([]byte, headerSize)
	_, err = data.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}

	// TODO check magic header
	// TODO Optionally prefetch the index data
	return &bDiskHashTable{
		data:        data,
		itemCount:   uint64(binary.BigEndian.Uint32(buf[8:])),
		bucketCount: uint64(binary.BigEndian.Uint32(buf[4:])),
		dataPos:     uint64(binary.BigEndian.Uint32(buf[12:])),
		id:          id,
	}, nil
}

var _ Table = (*bDiskHashTable)(nil)
