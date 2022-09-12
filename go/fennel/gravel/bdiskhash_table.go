package gravel

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fennel/lib/timer"
	"fennel/lib/utils"
	"fennel/lib/utils/math"
	"fmt"
	"os"
	"path"
	"runtime"
	"sort"
	"syscall"

	"go.uber.org/atomic"
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

var incompleteFile = fmt.Errorf("expected end of file")

type bDiskHashTable struct {
	data        []byte
	itemCount   uint64
	bucketCount uint64
	dataPos     uint64
	id          uint64
	reads       atomic.Uint64
}

func (b *bDiskHashTable) DataReads() uint64 {
	return b.reads.Load() * sampleRate
}

func (b *bDiskHashTable) Get(key []byte, hash uint64) (Value, error) {
	bucketCount := b.bucketCount
	var bucketId uint64
	if bucketCount&(bucketCount-1) == 0 {
		bucketId = hash & (bucketCount - 1)
	} else {
		bucketId = hash % b.bucketCount
	}

	pos := int(headerSize + bucketId*4)
	if len(b.data) < pos+4 {
		return Value{}, incompleteFile
	}

	relativeBucketOffset := binary.BigEndian.Uint32(b.data[pos:])
	if relativeBucketOffset == invalidUint32 {
		return Value{}, ErrNotFound
	}
	bucketOffset := uint64(relativeBucketOffset) + headerSize + b.bucketCount*4

	pos = int(bucketOffset)
	if len(b.data) <= pos {
		return Value{}, incompleteFile
	}

	itemsInBucket := int(b.data[pos])
	pos += 1
	if itemsInBucket == 255 {
		if len(b.data) < pos+4 {
			return Value{}, incompleteFile
		}
		itemsInBucket = int(binary.BigEndian.Uint32(b.data[pos:]))
		pos += 4
	}

	if len(b.data) < pos+5+itemsInBucket*4 {
		return Value{}, incompleteFile
	}

	bufCurrBucket := b.data[pos : pos+5+itemsInBucket*4]
	dataPos := (uint64(bufCurrBucket[0]) << 32) | (uint64(bufCurrBucket[1]) << 24) | (uint64(bufCurrBucket[2]) << 16) | (uint64(bufCurrBucket[3]) << 8) | (uint64(bufCurrBucket[4]))
	hashFP := uint32((hash >> 32) & 0xFFFFFFFF)

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
	return func() (Value, error) {
		sample := shouldSample()
		maybeInc(sample, &b.reads)
		if sample {
			_, t := timer.Start(context.TODO(), 1, "gravel.table.dataread")
			defer t.Stop()
		}
		pos = int(dataPos + b.dataPos)
		matchIdx := 0
		for i := 0; ; i++ {
			if i >= itemsInBucket {
				panic("file is inconsistent state")
			}
			keyPos := pos + 4 + 1 + 4 + 1 + 4
			if len(b.data) < keyPos {
				return Value{}, incompleteFile
			}

			// write record: [4 bytes total size, 1 byte key size, 4 bytes value size, 1 byte delete tombstone, 4 bytes expire time, key, value]
			recordSize := int(binary.BigEndian.Uint32(b.data[pos:]))

			if i == matchIndices[matchIdx] {
				keyLen := int(b.data[pos+4])
				if len(b.data) < keyPos+keyLen {
					return Value{}, incompleteFile
				}
				curKey := b.data[keyPos : keyPos+keyLen]
				if bytes.Equal(curKey, key) {
					// found
					valueSize := int(binary.BigEndian.Uint32(b.data[pos+5:]))
					deleted := b.data[pos+9] > 0
					expTime := binary.BigEndian.Uint32(b.data[pos+10:])
					if len(b.data) < keyPos+keyLen+valueSize {
						return Value{}, incompleteFile
					}

					value := b.data[keyPos+keyLen : keyPos+keyLen+valueSize]
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
			pos += recordSize
		}
		return Value{}, ErrNotFound
	}()
}

func (b *bDiskHashTable) Close() error {
	if b.data == nil {
		return nil
	}
	data := b.data
	b.data = nil
	runtime.SetFinalizer(b, nil)
	return syscall.Munmap(data)
}

func (b *bDiskHashTable) ID() uint64 {
	return b.id
}

func buildBDiskHashTable(dirname string, numShards uint64, mt *Memtable) ([]string, error) {
	fmt.Printf("num shards is: %d\n", numShards)
	filenames := make([]string, uint(numShards))
	for i := 0; i < int(numShards); i++ {
		filename := fmt.Sprintf("%d_%s%s", i, utils.RandString(8), tempSuffix)
		filepath := path.Join(dirname, filename)
		filenames[i] = filename
		fmt.Printf("starting to build the table: %s...\n", filepath)

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

		buildFunc := func(i uint) error {
			m := mt.Iter(uint64(i))

			itemCount := len(m)
			bucketCount := uint64(itemCount / int(avgBucketSize))
			bucketCount = math.NextPowerOf2(bucketCount)
			headSlice := make([]byte, bucketCount*4)
			for i := 0; i < int(bucketCount*4); i++ {
				headSlice[i] = 0xFF
			}

			indexObjs := make([]indexObj, len(m))
			idx := 0
			for key, value := range m {
				hash := Hash([]byte(key))
				indexObjs[idx] = indexObj{
					HashFP:   uint32((hash >> 32) & 0xFFFFFFFF),
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
				if _, err = dataWriter.Write(recordBuf); err != nil {
					return err
				}
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

		if err = buildFunc(uint(i)); err != nil {
			return nil, err
		}
		// sync the file to disk before going to avoid any data loss
		if err = f.Sync(); err != nil {
			return nil, err
		}
		err = f.Close()
		if err != nil {
			return nil, err
		}
	}
	return filenames, nil
}

func openBDiskHashTable(id uint64, filepath string) (Table, error) {
	var data []byte = nil

	f, err := os.Open(filepath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	if size <= int64(headerSize) {
		return nil, fmt.Errorf("file size too small to be a valid gravel file")
	}

	data, err = syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	dataPos := uint64(binary.BigEndian.Uint32(data[12:]))
	if len(data) <= int(dataPos) {
		_ = syscall.Munmap(data)
		return nil, err
	}

	for i := 0; i < int(dataPos); i++ {
		// Prefetch the index data [0, dataPos) by "touching" it
		_ = data[i]
	}

	tableObj := &bDiskHashTable{
		data:        data,
		itemCount:   uint64(binary.BigEndian.Uint32(data[8:])),
		bucketCount: uint64(binary.BigEndian.Uint32(data[4:])),
		dataPos:     dataPos,
		id:          id,
	}
	runtime.SetFinalizer(tableObj, (*bDiskHashTable).Close)
	return tableObj, nil
}

var _ Table = (*bDiskHashTable)(nil)
