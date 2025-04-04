package gravel

/*
	This file implements a disk based hash table optimized for immutable files.
	It optimizes for the case where keys are not present in the table - in such
	cases, it only takes one index read (and two for ~5% of all requests).
	The index is small enough that we can keep the entire index in RAM.
	As a result, the common case of absent key is blazing fast. When the key is
	present, besides 1-2 RAM lookups, it takes upto one disk read (and even that
	may be avoided if the data section is cached in RAM).

	Contents of hashtable are mmaped and so if the machine has enough RAM, ALL the
	lookups can be served via memory only.

	Here is how the table is laid out in disk
		Header (64 bytes)
		Data (variable size, followed by padding to make sure the end-of-section is aligned with 64 bytes)
		L1 Index (variable size - fixed 32 bytes for each hash bucket)
		L2 Index (variable size - single block for all index data that doesn't fit in L1 index)
        4 bytes MagicTailer

	Given a key, we find the bucket that that it goes to using it hash and the number
	of total buckets (stored in header). Then we jump to L1 index for that bucket. That
	bucket contains a bunch of fingerprints of keys living in that bucket. It also stores
	the offset in the data section where the actual data lives. If 32 bytes are sufficient
	for all fingerprints, we will directly jump to data section if any fingerprint matches.
	If 32 bytes aren't sufficient, it contains the offset within L2 section where
	the rest of the fingerprints are stored (this happens for <8% of keys only)

	Header block stores summary of the table. Here is its structure:
		4 bytes MagicHeader
		1 bytes codec
		1 bytes encrypted (boolean - 0 or 1, currently 0 for all tables since we don't do encryption)
		1 bytes compression type (currently this is zero for all tables since we don't do compression)
		1 bytes number of bits used in sharding
		4 bytes number of records (implies that no table can have more than 4B items)
		4 bytes number of hash buckets (roughly item_count / 8)
		8 bytes data size
		4 bytes index size
		4 bytes min expiration timestamp of table (this helps to compress expiration time of entries in <4 bytes)
		4 bytes max expiration timestamp of table (this helps us wholesale expire a table when the time is right)
		4 bytes data checksum (TODO)
		4 bytes level 1 index checksum (TODO)
		4 bytes level 2 index checksum (TODO)
		filler bytes to make the total header size 64

	1st level index can be thought of as literally a flat list of 32 bytes - one entry for each
	hash bucket. The structure of these 32 bytes are as follows:
		1 byte - number of keys in this bucket
		5 bytes for the location of the actual data - expressed as offset within data section
		Then if number of keys <= 13:
			One 2 byte fingerprint for all keys one after one
		Else:
			4 bytes of the location in overflow where the remaining keys will be present
			Followed by 2 byte fingerprint for first 11 keys (remaining fingerprints will be in overflow)

	2nd level index stores any remaining fingerprints that could not fit within 32 bytes of L1 entry.
	Each fingerprint is fixed 2 bytes.

	Data section stores a flat list of all the entries like below:
    note that for records in each bucket, keys and metadata are grouped together

		total size of keys varint

		key size 1 (varint)
		key 1
        deletion tombstone 1
		value size 1 (varint)
		Delta between expire time and min expire time of the table (varint)

		key size 2 (varint)
		key 2
        deletion tombstone 2
		value size 2 (varint)
		Delta between expire time and min expire time of the table (varint)
		...
		key size N (varint)
		key N
        deletion tombstone N
		value size N (varint)
		Delta between expire time and min expire time of the table (varint)

		value1
        value2
		...
        valueN

	Note if deletion tombstone is true, we don't store the rest (exptime, value size, and actual value) of the fields of the entry.
*/

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"fennel/lib/timer"
	fbinary "fennel/lib/utils/binary"
	"fennel/lib/utils/slice"
	"fmt"
	math2 "math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"
	"time"
	"unsafe"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	magicHeader                  uint32 = 0x24112021 // this is just an arbitrary 32 bit number - day Fennel was incorporated :)
	magicTailer                  uint32 = 0x20211124 // this is just an arbitrary 32 bit number - day Fennel was incorporated :)
	v1codec_xxhash               uint8  = 1
	numRecordsPerBucket          uint32 = 9
	fileHeaderSize               int    = 64
	bucketSizeBytes              int    = 32 // half of a cache line
	maxBucketFpCount             int    = 13
	expiryPercentileSamplingSize int    = 10000
)

var incompleteFile = fmt.Errorf("expected end of file")

// TODO: remove once this value is consistently ZERO
var corruptedHashtables = promauto.NewCounter(
	prometheus.CounterOpts{
		Name: "corrupted_hashtables",
		Help: "Number of hashtables which are in corrupted state i.e. with numBuckets == 1, but indexSize == 0",
	},
)

type fingerprint uint16

type header struct {
	datasize    uint64
	indexsize   uint32
	numRecords  uint32
	numBuckets  uint32
	minExpiry   Timestamp
	maxExpiry   Timestamp
	magic       uint32
	codec       uint8
	encrypted   bool
	compression uint8
	expiryP25   Timestamp
	expiryP50   Timestamp
	expiryP75   Timestamp
}

// record denotes a k-v pair that exists within the table, for internal use only when building the table
type record struct {
	bucketID uint32
	fp       fingerprint
	key      string
	value    Value
}

// a bucket denotes a hash bucket in the table and contains a list of
// all the records that are stored in that bucket along with the offset
// in the file where this bucket's data is stored
type bucket struct {
	bucketID    uint32
	dataStart   uint64
	firstRecord int
	lastRecord  int
}

type hashTable struct {
	name       string
	head       header
	mappedData []byte
	index      []byte // derived from mmappedData
	overflow   []byte // derived from mmappedData
	data       []byte // derived from mmappedData
	size       uint64
}

func (ht *hashTable) ShouldGCExpired() bool {
	now := time.Now().Unix()
	return int64(ht.head.expiryP50) < now
}

func (ht *hashTable) IndexSize() uint64 {
	return uint64(ht.head.indexsize)
}

func (ht *hashTable) Name() string {
	return ht.name
}

func (ht *hashTable) NumRecords() uint64 {
	return uint64(ht.head.numRecords)
}

func (ht *hashTable) GetAll(m map[string]Value) error {
	data := ht.data

	pos := uint64(0)
	recordCnt := 0
	for {
		keys := data[pos:]

		vPos := 0
		keysTotalLen, headLen, err := fbinary.ReadUvarint(keys)
		if err != nil {
			return incompleteFile
		}
		values := keys[headLen+int(keysTotalLen):]

		keys = keys[headLen:]
		kPos := 0
		for kPos < int(keysTotalLen) {
			keyLen, n, err := fbinary.ReadUvarint(keys[kPos:])
			if err != nil {
				return incompleteFile
			}
			kPos += n
			curKey := keys[kPos : kPos+int(keyLen)]
			kPos += int(keyLen)
			deleted := keys[kPos]
			kPos++

			var value Value
			if deleted > 0 {
				value = Value{data: []byte{}, expires: 0, deleted: true}
			} else {
				valLen, n, err := fbinary.ReadUvarint(keys[kPos:])
				if err != nil {
					return incompleteFile
				}
				kPos += n

				expiry64, n, err := fbinary.ReadUvarint(keys[kPos:])
				if err != nil {
					return incompleteFile
				}
				kPos += n
				expiry := Timestamp(expiry64)
				if expiry > 0 {
					expiry = ht.head.minExpiry + expiry - 1
				}
				value = Value{
					data:    values[vPos : vPos+int(valLen)],
					expires: expiry,
					deleted: false,
				}
				vPos += int(valLen)
			}
			recordCnt++
			m[*(*string)(unsafe.Pointer(&curKey))] = value
		}
		pos += uint64(headLen + kPos + vPos)
		if recordCnt >= int(ht.head.numRecords) {
			break
		}
	}
	return nil
}

func (ht *hashTable) Size() uint64 {
	return ht.size
}

func (ht *hashTable) Get(key []byte, hash uint64) (Value, error) {
	head := ht.head // deref in local variables to reduce address translations

	// This is to avoid the bug introduced in https://github.com/fennel-ai/rexer/pull/1589 where indexSize
	// could be ZERO but numBuckets == 1
	//
	// Remove this once the compaction has removed all the tables with the above state
	if head.numBuckets == 0 {
		// nothing to read here
		return Value{}, ErrNotFound
	}
	bucketID := getBucketID(hash, head.numBuckets)
	fp := getFingerprint(hash)

	// even if numBuckets > 0, it is possible that index of the hashTable is empty, so we make that check in this method
	matchIndex, matchCount, numCandidates, dataPos, err := ht.readIndex(bucketID, fp)
	if err != nil {
		return Value{}, err
	}
	if matchIndex < 0 {
		return Value{}, ErrNotFound
	}
	return ht.readData(dataPos, matchIndex, matchCount, numCandidates, key, head.minExpiry)
}

// readIndex reads the index for bucketID and searches for the given fingerprint.
// returns the slice of indices (in current bucket) whose fingerprints matches the given, the total number of records that
// fall into this bucket, as well as the data section offset, with error if there is any
func (ht *hashTable) readIndex(bucketID uint32, fp fingerprint) (int, int, int, uint64, error) {
	// index is empty, so nothing to match here
	//
	// This is to avoid the bug introduced in https://github.com/fennel-ai/rexer/pull/1589 where indexSize
	// could be ZERO but numBuckets == 1
	//
	// Remove this once the compaction has removed all the tables with the above state
	if len(ht.index) == 0 {
		return -1, 0, 0, 0, nil
	}

	indexStart := int(bucketID) * bucketSizeBytes
	indexEnd := indexStart + bucketSizeBytes
	index := ht.index[indexStart:indexEnd]

	numKeys := int(index[0])
	datapos := uint64(index[1])<<32 + uint64(index[2])<<24 + uint64(index[3])<<16 + uint64(index[4])<<8 + uint64(index[5])
	var overflow uint32
	sofar := 6

	fpInBucket := numKeys
	// if the bucket can't hold all keys, read overflow offset
	if numKeys > maxBucketFpCount {
		overflow = binary.LittleEndian.Uint32(index[sofar:])
		sofar += 4
		fpInBucket = maxBucketFpCount - 2
	}
	// now compare all sorted fps one by one until seeing a bigger value which indicates a stop
	curFpPos := 0
	fpB1, fpB2 := byte(fp>>8), byte(fp)

	matchFpPos := -1
	matchFpCount := 0

	for ; curFpPos < fpInBucket; curFpPos += 1 {
		if index[sofar] > fpB1 {
			return matchFpPos, matchFpCount, numKeys, datapos, nil
		} else if index[sofar] == fpB1 {
			if index[sofar+1] == fpB2 {
				if matchFpPos < 0 {
					matchFpPos = curFpPos
				}
				matchFpCount += 1
			}
		}
		sofar += 2
	}
	if curFpPos >= numKeys {
		// no overflow record needs to be compared
		return matchFpPos, matchFpCount, numKeys, datapos, nil
	}

	// there are extra fingerprints that are in overflow section and need to check
	index = ht.overflow[overflow:]
	sofar = 0
	for ; curFpPos < numKeys; curFpPos += 1 {
		if index[sofar] > fpB1 {
			return matchFpPos, matchFpCount, numKeys, datapos, nil
		} else if index[sofar] == fpB1 {
			if index[sofar+1] == fpB2 {
				if matchFpPos < 0 {
					matchFpPos = curFpPos
				}
				matchFpCount += 1
			}
		}
		sofar += 2
	}
	return matchFpPos, matchFpCount, numKeys, datapos, nil
}

// writeIndex writes all the index data via writer and returns the number of bytes
// written and the error if any. This assumes that both l2entries and records are
// sorted on bucketID
func writeIndex(writer *bufio.Writer, numBuckets uint32, l2entries []bucket, records []record) (uint32, error) {
	// if there are no records, no need for an index as well
	if len(records) == 0 {
		return 0, nil
	}

	overflow := make([]byte, 0, 2*numBuckets*8) // reserve some size (but not too large, 8 times the number of buckets) to avoid too much copying
	entry := make([]byte, 32)
	for bid := uint32(0); bid < numBuckets; bid++ {
		slice.Fill(entry, 0)

		l2entry := l2entries[bid]
		numKeys := l2entry.lastRecord - l2entry.firstRecord
		if numKeys > 255 {
			return 0, fmt.Errorf("number of keys in the bucket greater than 255: %d, this is not supported", numKeys)
		}
		// first write the number of keys in this bucket
		entry[0] = byte(numKeys)

		if numKeys > 0 {
			// not an empty bucket
			// write the position of this bucket's data (as offset within data segment) in 5 bytes
			datapos := l2entry.dataStart
			if datapos >= (1 << 40) {
				return 0, fmt.Errorf("value too large, doesn't fit in 5 bytes")
			}
			entry[1] = byte(datapos >> 32)
			entry[2] = byte(datapos >> 24)
			entry[3] = byte(datapos >> 16)
			entry[4] = byte(datapos >> 8)
			entry[5] = byte(datapos)
			sofar := 6

			fpToPutInBucket := numKeys
			if numKeys > maxBucketFpCount {
				// followed by overflow section offset, if keys can't feed into the bucket
				binary.LittleEndian.PutUint32(entry[sofar:], uint32(len(overflow)))
				sofar += 4
				fpToPutInBucket = maxBucketFpCount - 2 // bucket can't hold maxBucketFpCount fingerprints anymore, reduce by one
			}

			curRecord := l2entry.firstRecord
			for ; curRecord < fpToPutInBucket+l2entry.firstRecord; curRecord += 1 {
				entry[sofar] = byte(records[curRecord].fp >> 8)
				entry[sofar+1] = byte(records[curRecord].fp)
				sofar += 2
			}

			// and if any were left, write them to the overflow section
			for ; curRecord < numKeys+l2entry.firstRecord; curRecord += 1 {
				overflow = append(overflow, byte(records[curRecord].fp>>8), byte(records[curRecord].fp))
			}
		}

		// now write this entry to the writer
		if _, err := writer.Write(entry); err != nil {
			return 0, err
		}
	}
	// now write all the overflow section
	if _, err := writer.Write(overflow); err != nil {
		return 0, err
	}
	return uint32(bucketSizeBytes)*numBuckets + uint32(len(overflow)), nil
}

// readData reads the data segment starting at 'start' and read upto numRecords to find a
// record that matches given key. If found, the value is returned, else err is set to ErrNotFound
func (ht *hashTable) readData(start uint64, matchIndex int, matchCount int, numRecords int, key []byte, minExpiry Timestamp) (Value, error) {
	if shouldSample() {
		_, t := timer.Start(context.TODO(), 1, "gravel.table.dataread")
		defer t.Stop()
	}

	keys := ht.data[start:]
	kPos := 0
	keysTotalLen, n, err := fbinary.ReadUvarint(keys[kPos:])
	if err != nil {
		return Value{}, incompleteFile
	}
	kPos += n
	values := keys[kPos+int(keysTotalLen):]

	valuePos := uint64(0)
	for i := 0; i < numRecords; i++ {
		keyLen, n, err := fbinary.ReadUvarint(keys[kPos:])
		if err != nil {
			return Value{}, incompleteFile
		}
		kPos += n
		curKey := keys[kPos : kPos+int(keyLen)]
		kPos += int(keyLen)
		deleted := keys[kPos]
		kPos++
		valLen := uint64(0)
		expiry := Timestamp(0)
		if deleted > 0 {
			// pass
		} else {
			valLen, n, err = fbinary.ReadUvarint(keys[kPos:])
			if err != nil {
				return Value{}, incompleteFile
			}
			kPos += n

			expiry64, n, err := fbinary.ReadUvarint(keys[kPos:])
			if err != nil {
				return Value{}, incompleteFile
			}
			kPos += n
			expiry = Timestamp(expiry64)
			if expiry > 0 {
				expiry = minExpiry + expiry - 1
			}
		}

		if i >= matchIndex {
			if i >= matchIndex+matchCount {
				break
			}
			// fingerprint match, a potential hit
			if bytes.Equal(key, curKey) {
				if deleted > 0 {
					return Value{data: []byte{}, expires: 0, deleted: true}, nil
				} else {
					return Value{
						data:    clonebytes(values[valuePos : valuePos+valLen]),
						expires: expiry,
						deleted: false,
					}, nil
				}
			}
		}
		valuePos += valLen
	}
	return Value{}, ErrNotFound
}

// It returns the total number of bytes written and a list of bucket which basically captures
// the starting and ending record for each bucket
func writeData(writer *bufio.Writer, records []record, minExpiry Timestamp, numBuckets uint32) (uint64, []bucket, error) {
	if len(records) == 0 {
		return 0, nil, nil
	}
	l2entries := make([]bucket, numBuckets)

	bucketID := uint32(0) // bucket ID of the current bucket
	for i, r := range records {
		if i == 0 || r.bucketID == bucketID {
			// bucket continues
			l2entries[bucketID].lastRecord += 1
		} else {
			// new bucket is opening now
			bucketID = r.bucketID
			l2entries[bucketID].bucketID = bucketID
			l2entries[bucketID].firstRecord = i
			l2entries[bucketID].lastRecord = i + 1
		}
	}

	offset := uint64(0)
	keyBuf := make([]byte, 0, 4096)
	valueBuf := make([]byte, 0, 4096*numRecordsPerBucket) // just preallocate some arbitrary sizes
	for i := range l2entries {
		if l2entries[i].firstRecord == l2entries[i].lastRecord {
			// indicates an empty bucket, don't encode anything
			continue
		}
		l2entries[i].dataStart = offset

		keyBuf = keyBuf[:0] // clear the buf
		valueBuf = valueBuf[:0]
		for rIdx := l2entries[i].firstRecord; rIdx < l2entries[i].lastRecord; rIdx++ {
			writeRecord(&keyBuf, &valueBuf, &records[rIdx], minExpiry)
		}

		keysLenBuf := make([]byte, 0, 8)
		writeUvarint(&keysLenBuf, uint64(len(keyBuf)))
		if _, err := writer.Write(keysLenBuf); err != nil {
			return offset, l2entries, fmt.Errorf("failed to write data segment: %w", err)
		}
		offset += uint64(len(keysLenBuf))
		if _, err := writer.Write(keyBuf); err != nil {
			return offset, l2entries, fmt.Errorf("failed to write data segment: %w", err)
		}
		offset += uint64(len(keyBuf))
		if _, err := writer.Write(valueBuf); err != nil {
			return offset, l2entries, fmt.Errorf("failed to write data segment: %w", err)
		}
		offset += uint64(len(valueBuf))
	}
	return offset, l2entries, nil
}

func (ht *hashTable) Close() error {
	var ret error = nil
	if ht.mappedData != nil {
		ht.data = nil
		ht.index = nil
		data := ht.mappedData
		ht.mappedData = nil
		if err := syscall.Munmap(data); err != nil {
			ret = err
		}
	}
	runtime.SetFinalizer(ht, nil)
	return ret
}

var _ Table = (*hashTable)(nil)

func getRecords(data map[string]Value, numBuckets uint32) []record {
	bucketEntries := make([][]record, numBuckets)
	for i := range bucketEntries {
		bucketEntries[i] = make([]record, 0, 2*numRecordsPerBucket)
	}
	for k, v := range data {
		hash := Hash([]byte(k))
		bucketID := getBucketID(hash, numBuckets)
		bucketEntries[bucketID] = append(bucketEntries[bucketID], record{
			bucketID: getBucketID(hash, numBuckets),
			fp:       getFingerprint(hash),
			key:      k,
			value:    v,
		})
	}
	// Sort entries within each bucket by their fingerprint so that we
	// can do early termination in the get path. We use manual insertion sort
	// instead of standard slice.Sort to keep the overhead low and save CPU
	for _, entries := range bucketEntries {
		for i := 1; i < len(entries); i++ {
			fp := entries[i].fp
			for j := i; j >= 0; j-- {
				if j >= 1 && entries[j-1].fp > fp {
					continue
				}
				e := entries[i]
				copy(entries[j+1:], entries[j:i])
				entries[j] = e
				break
			}
		}
	}
	i := 0
	records := make([]record, len(data))
	for _, entries := range bucketEntries {
		i += copy(records[i:], entries)
	}
	return records
}

func buildHashTable(filepath string, data map[string]Value) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	numRecords := uint32(len(data))
	numBuckets := numRecords / numRecordsPerBucket

	if numRecords == 0 {
		numBuckets = 0
	} else if numBuckets == 0 {
		// when numRecords < numRecordsPerBucket
		numBuckets = 1
	}

	minExpiry := Timestamp(math2.MaxUint32)
	maxExpiry := Timestamp(0)
	records := getRecords(data, numBuckets)

	expTimeSampled := make([]Timestamp, 0, expiryPercentileSamplingSize)
	sampledIdx := 0
	for _, r := range records {
		v := r.value
		if sampledIdx < expiryPercentileSamplingSize {
			if !v.deleted {
				if v.expires > 0 {
					expTimeSampled = append(expTimeSampled, v.expires)
				} else {
					expTimeSampled = append(expTimeSampled, math2.MaxUint32)
				}
				sampledIdx += 1
			}
		}
		if v.expires > 0 {
			if minExpiry > v.expires {
				minExpiry = v.expires
			}
			if maxExpiry < v.expires {
				maxExpiry = v.expires
			}
		}
	}
	sort.Slice(expTimeSampled, func(i, j int) bool { return expTimeSampled[i] < expTimeSampled[j] })
	if len(expTimeSampled) == 0 {
		expTimeSampled = append(expTimeSampled, math2.MaxUint32) // avoid crash due to empty sampling
	}

	// now start writing the data section starting byte 64 (leaving bytes 0 - 63 for header)
	_, err = f.Seek(int64(fileHeaderSize), unix.SEEK_SET)
	if err != nil {
		return err
	}
	writer := bufio.NewWriterSize(f, 1024*1024)
	datasize, buckets, err := writeData(writer, records, minExpiry, numBuckets)
	if err != nil {
		return err
	}

	// We want index data to be 64 byte aligned, so if data size is not a multiple of 64, add some filler
	gap := 64 - (datasize & 63)
	if gap > 0 {
		if _, err = writer.Write(make([]byte, gap)); err != nil {
			return fmt.Errorf("failed to write data segment: %w", err)
		}
		datasize += gap
	}

	// now write the index
	indexsize, err := writeIndex(writer, numBuckets, buckets, records)
	if err != nil {
		return fmt.Errorf("failed to write index segment: %w", err)
	}
	// write tail magic
	tailMagicBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(tailMagicBytes, magicTailer)
	if _, err = writer.Write(tailMagicBytes); err != nil {
		return fmt.Errorf("failed to write magictailer: %w", err)
	}
	// flush any existing data in the writer and then go to the start to write the header
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("error flushing the buffered writer %w", err)
	}

	// write header
	_, err = f.Seek(0, unix.SEEK_SET)
	if err != nil {
		return err
	}
	writer.Reset(f)
	head := header{
		magic:       magicHeader,
		codec:       v1codec_xxhash,
		encrypted:   false,
		compression: 0,
		numRecords:  numRecords,
		numBuckets:  numBuckets,
		datasize:    datasize,
		indexsize:   indexsize,
		minExpiry:   minExpiry,
		maxExpiry:   maxExpiry,
		expiryP25:   expTimeSampled[len(expTimeSampled)/4],
		expiryP50:   expTimeSampled[len(expTimeSampled)/2],
		expiryP75:   expTimeSampled[(len(expTimeSampled)*3)/4],
	}
	if err = writeHeader(writer, head); err != nil {
		return err
	}

	if err = writer.Flush(); err != nil {
		return fmt.Errorf("error flushing the buffered writer %w", err)
	}
	if err = f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func writeHeader(writer *bufio.Writer, head header) error {
	buf := make([]byte, fileHeaderSize)
	binary.LittleEndian.PutUint32(buf, head.magic)
	buf[4] = head.codec
	// false for encryption
	if head.encrypted {
		buf[5] = 1
	}
	buf[6] = head.compression
	binary.LittleEndian.PutUint32(buf[7:], head.numRecords)
	binary.LittleEndian.PutUint32(buf[11:], head.numBuckets)
	binary.LittleEndian.PutUint64(buf[15:], head.datasize)
	binary.LittleEndian.PutUint32(buf[23:], head.indexsize)
	binary.LittleEndian.PutUint32(buf[27:], uint32(head.minExpiry))
	binary.LittleEndian.PutUint32(buf[31:], uint32(head.maxExpiry))
	binary.LittleEndian.PutUint32(buf[35:], uint32(head.expiryP25))
	binary.LittleEndian.PutUint32(buf[39:], uint32(head.expiryP50))
	binary.LittleEndian.PutUint32(buf[43:], uint32(head.expiryP75))
	if _, err := writer.Write(buf); err != nil {
		return err
	}
	return nil
}

func readHeader(buf []byte) (header, error) {
	if len(buf) != fileHeaderSize {
		return header{}, fmt.Errorf("header buffer of incorrect size")
	}
	var head header
	head.magic = binary.LittleEndian.Uint32(buf)
	if head.magic != magicHeader {
		return head, fmt.Errorf("header's magic %d doesn't match expected magic: %d", head.magic, magicHeader)
	}
	head.codec = buf[4]
	if head.codec != v1codec_xxhash {
		return head, fmt.Errorf("invalid codec")
	}
	if buf[5] > 0 {
		head.encrypted = true
	}
	head.compression = buf[6]
	head.numRecords = binary.LittleEndian.Uint32(buf[7:])
	head.numBuckets = binary.LittleEndian.Uint32(buf[11:])
	head.datasize = binary.LittleEndian.Uint64(buf[15:])
	head.indexsize = binary.LittleEndian.Uint32(buf[23:])
	head.minExpiry = Timestamp(binary.LittleEndian.Uint32(buf[27:]))
	head.maxExpiry = Timestamp(binary.LittleEndian.Uint32(buf[31:]))
	head.expiryP25 = Timestamp(binary.LittleEndian.Uint32(buf[35:]))
	head.expiryP50 = Timestamp(binary.LittleEndian.Uint32(buf[39:]))
	head.expiryP75 = Timestamp(binary.LittleEndian.Uint32(buf[43:]))
	return head, nil
}

func writeRecord(keyBuf *[]byte, valueBuf *[]byte, r *record, minExpiry Timestamp) {
	writeUvarint(keyBuf, uint64(len(r.key)))
	*keyBuf = append(*keyBuf, r.key...)
	if r.value.deleted {
		*keyBuf = append(*keyBuf, 1)
		return
	}
	*keyBuf = append(*keyBuf, 0)
	writeUvarint(keyBuf, uint64(len(r.value.data)))
	if r.value.expires == 0 {
		*keyBuf = append(*keyBuf, 0)
	} else {
		writeUvarint(keyBuf, uint64(r.value.expires-minExpiry+1))
	}

	*valueBuf = append(*valueBuf, r.value.data...)
}

// take the arbitrary middle bits from the hash and trim to 16 bits
func getFingerprint(h uint64) fingerprint {
	return fingerprint(h >> 29)
}

func writeUvarint(buf *[]byte, x uint64) uint32 {
	i := uint32(0)
	for x >= 0x80 {
		*buf = append(*buf, byte(x)|0x80)
		x >>= 7
		i += 1
	}
	*buf = append(*buf, byte(x))
	return i + 1
}

func openHashTable(fullFileName string, warmIndex bool, warmData bool) (Table, error) {
	name := filepath.Base(fullFileName)
	f, err := os.Open(fullFileName)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := fi.Size()
	if size <= int64(fileHeaderSize) {
		return nil, fmt.Errorf("file size too small to be a valid gravel file")
	}

	buf, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}

	header, err := readHeader(buf[:64])
	if err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}
	expectedSize := int64(header.datasize) + int64(header.indexsize) + int64(fileHeaderSize) + 4
	if size != expectedSize {
		_ = syscall.Munmap(buf)
		return nil, fmt.Errorf("incorrect file size, actual %d vs expected %d", size, expectedSize)
	}
	if size != int64(len(buf)) || binary.LittleEndian.Uint32(buf[size-4:]) != magicTailer {
		_ = syscall.Munmap(buf)
		return nil, fmt.Errorf("invalid file tail magic or mmapping unsuccessful")
	}
	dataStart := uint64(fileHeaderSize)
	dataEnd := dataStart + header.datasize
	data := buf[dataStart:dataEnd]
	madviseData := buf[0:dataEnd] // madvise must use page-aligned ptr

	indexEnd := dataEnd + uint64(header.indexsize)
	index := buf[dataEnd:indexEnd]
	madviseIndex := buf[dataEnd & ^(uint64(os.Getpagesize())-1) : indexEnd] // madvise must use page-aligned ptr

	// This is to avoid the bug introduced in https://github.com/fennel-ai/rexer/pull/1589 where indexSize
	// could be ZERO but numBuckets == 1
	//
	// Remove this once the compaction has removed all the tables with the above state
	var overflowIdx int
	if header.indexsize == 0 {
		overflowIdx = 0
	} else {
		overflowIdx = int(header.numBuckets)*bucketSizeBytes
	}
	overflow := index[overflowIdx:]
	err = unix.Madvise(madviseIndex, syscall.MADV_WILLNEED)
	if err != nil {
		zap.L().Error("failed to Madvise on index mapping", zap.String("filename", fullFileName), zap.Error(err))
	}
	err = unix.Madvise(madviseData, syscall.MADV_RANDOM)
	if err != nil {
		zap.L().Error("failed to Madvise on data mapping", zap.String("filename", fullFileName), zap.Error(err))
	}
	madviseIndex = nil // nolint
	madviseData = nil  // nolint  avoid pointer leak in mmap

	// Prefetch both the index and data by "touching" it
	if warmIndex {
		for i := 0; i < len(index); i++ {
			_ = index[i]
		}
	}

	if warmData {
		for i := 0; i < len(data); i++ {
			_ = data[i]
		}
	}

	if header.numBuckets > 0 && len(index) == 0 {
		corruptedHashtables.Inc()
	}

	tableObj := &hashTable{
		name:       name,
		head:       header,
		overflow:   overflow,
		mappedData: buf,
		index:      index,
		data:       data,
		size:       uint64(size),
	}
	tableInfo := fmt.Sprintf("numRecords:%d,totalSize:%d,indexSize:%d,minExp:%d,maxExp:%d,expP25:%d,expP50:%d,expP75:%d",
		header.numRecords, size, header.indexsize, header.minExpiry, header.maxExpiry, header.expiryP25, header.expiryP50, header.expiryP75)
	zap.L().Info("opened hash table", zap.String("filename", fullFileName), zap.String("info", tableInfo))
	runtime.SetFinalizer(tableObj, (*hashTable).Close)
	return tableObj, nil
}

func getBucketID(hash uint64, numBuckets uint32) uint32 {
	return uint32(hash % uint64(numBuckets))
}
