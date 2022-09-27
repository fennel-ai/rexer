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
		L1 Index (variable size - fixed 64 bytes for each hash bucket)
		L2 Index (variable size - single block for all index data that doesn't fit in L1 index)
        4 bytes MagicTailer

	Given a key, we find the bucket that that it goes to using it hash and the number
	of total buckets (stored in header). Then we jump to L1 index for that bucket. That
	bucket contains a bunch of fingerprints of keys living in that bucket. It also stores
	the offset in the data section where the actual data lives. If 64 bytes are sufficient
	for all fingerprints, we will directly jump to data section if any fingerprint matches.
	If 64 bytes aren't sufficient, it contains the offset within L2 section where
	the rest of the fingerprints are stored.

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

	1st level index can be thought of as literally a flat list of 64 bytes - one entry for each
	hash bucket. The structure of these 64 bytes are as follows:
		1 byte - number of keys in this bucket
		5 bytes for the location of the actual data - expressed as offset within data section
        Then either 3 bytes * 19 == 57 bytes,  19 fingerprints, if number of keys <= 19, which means not all fingerprints can fit in the bucket
             or     4 bytes offset in L2 index of the overflow fingerprints, then 3 bytes * 18 = 54 bytes

	2nd level index stores any remaining fingerprints that could not fit within 64 bytes of L1 entry.
	Each fingerprint is fixed 3 bytes.

	Data section stores a flat list of all the entries like this:
		key size (varint)
		full key
		1 byte deletion tombstone
		4 bytes value size
		Delta between expire time and min expire time of the table (varint)
		full value

	Note if deletion tombstone is true, we don't store the rest of the fields of the entry.
*/

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
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
	"unsafe"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const (
	magicHeader         uint32 = 0x24112021 // this is just an arbitrary 32 bit number - day Fennel was incorporated :)
	magicTailer         uint32 = 0x20211124 // this is just an arbitrary 32 bit number - day Fennel was incorporated :)
	v1codec_xxhash      uint8  = 1
	numRecordsPerBucket uint32 = 19
	fileHeaderSize      int    = 64
	bucketSizeBytes     int    = 64 // a common cache line size
	maxBucketFpCount    int    = 28

	stackMatchedIdxArraySize int = 64
)

var incompleteFile = fmt.Errorf("expected end of file")

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

func (ht *hashTable) IndexSize() uint64 {
	return uint64(ht.head.indexsize)
}

func (ht *hashTable) Name() string {
	return ht.name
}

func (ht *hashTable) NumRecords() uint64 {
	return uint64(ht.head.numRecords)
}

func (ht *hashTable) GetAll() ([]Entry, error) {
	data := ht.data
	sofar := 0
	entries := make([]Entry, ht.head.numRecords)
	for i := range entries {
		keyLen, n, err := fbinary.ReadUvarint(data[sofar:])
		if err != nil {
			return nil, incompleteFile
		}
		sofar += n
		curKey := data[sofar : sofar+int(keyLen)]
		sofar += int(keyLen)
		v, n, err := readValue(data[sofar:], ht.head.minExpiry, false)
		if err != nil {
			return nil, incompleteFile
		}
		sofar += n
		entries[i] = Entry{key: curKey, val: v}
	}
	return entries, nil
}

func (ht *hashTable) Size() uint64 {
	return ht.size
}

func (ht *hashTable) Get(key []byte, hash uint64) (Value, error) {
	head := ht.head // deref in local variables to reduce address translations
	bucketID := getBucketID(hash, head.numBuckets)
	fp := getFingerprint(hash)

	// for performance consideration, limit the number of matchedIndices to be const so the values can be stored on stack
	// this causes the bug that if there are more than 'stackMatchedIdxArraySize' matched hashes, we may miss the record
	// however, in reality, this will (almost ,like 1 every 3e188) never trigger if stackMatchedIdxArraySize == 32
	matchIndex, numCandidates, dataPos, err := ht.readIndex(bucketID, fp)
	if err != nil {
		return Value{}, err
	}
	if matchIndex < 0 {
		return Value{}, ErrNotFound
	}
	ret, err := ht.readData(dataPos, matchIndex, numCandidates, key, head.minExpiry)
	return ret, err
}

// readIndex reads the index for bucketID and searches for the given fingerprint.
// returns the slice of indices (in current bucket) whose fingerprints matches the given, the total number of records that
// fall into this bucket, as well as the data section offset, with error if there is any
func (ht *hashTable) readIndex(bucketID uint32, fp fingerprint) (int, int, uint64, error) {
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
		fpInBucket = maxBucketFpCount - 1
	}
	// now compare all sorted fps one by one until seeing a bigger value which indicates a stop
	curFpPos := 0
	fpB1, fpB2 := byte(fp>>8), byte(fp)

	for ; curFpPos < fpInBucket; curFpPos += 1 {
		if index[sofar] > fpB1 { // no match, so matchIndex is -1
			return -1, numKeys, datapos, nil
		} else if index[sofar] == fpB1 {
			if index[sofar+1] == fpB2 {
				return curFpPos, numKeys, datapos, nil
			}
		}
		sofar += 2
	}
	if curFpPos >= numKeys {
		// no overflow record needs to be compared
		return -1, numKeys, datapos, nil
	}

	// there are extra fingerprints that are in overflow section and need to check
	index = ht.overflow[overflow:]
	sofar = 0
	for ; curFpPos < numKeys; curFpPos += 1 {
		if index[sofar] > fpB1 {
			return -1, numKeys, datapos, nil
		} else if index[sofar] == fpB1 {
			if index[sofar+1] == fpB2 {
				return curFpPos, numKeys, datapos, nil
			}
		}
		sofar += 2
	}
	// no match found until the very end
	return -1, numKeys, datapos, nil
}

// writeIndex writes all the index data via writer and returns the number of bytes
// written and the error if any. This assumes that both l2entries and records are
// sorted on bucketID
func writeIndex(writer *bufio.Writer, numBuckets uint32, l2entries []bucket, records []record) (uint32, error) {
	overflow := make([]byte, 0, 3*numBuckets*4) // reserve some size (but not too large, 4 times the number of buckets) to avoid too much copying
	entry := make([]byte, 64)
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
				fpToPutInBucket = maxBucketFpCount - 1 // bucket can't hold maxBucketFpCount fingerprints anymore, reduce by one
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
func (ht *hashTable) readData(start uint64, matchIndex int, numRecords int, key []byte, minExpiry Timestamp) (Value, error) {
	if shouldSample() {
		_, t := timer.Start(context.TODO(), 1, "gravel.table.dataread")
		defer t.Stop()
	}

	data := ht.data[start:]
	sofar := 0
	for i := 0; i < numRecords; i++ {
		keyLen, n, err := fbinary.ReadUvarint(data[sofar:])
		if err != nil {
			return Value{}, incompleteFile
		}
		sofar += n
		if i >= matchIndex {
			// fingerprint match, a potential hit
			curKey := data[sofar : sofar+int(keyLen)]
			sofar += int(keyLen)
			if bytes.Equal(key, curKey) {
				ret, _, err := readValue(data[sofar:], minExpiry, false)
				return ret, err
			}
		} else {
			sofar += int(keyLen)
		}
		_, m, err := readValue(data[sofar:], minExpiry, true)
		if err != nil {
			return Value{}, err
		}
		sofar += m
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
	offset := uint64(0)
	for i, r := range records {
		if i == 0 || r.bucketID == bucketID {
			// bucket continues
			l2entries[bucketID].lastRecord += 1
		} else {
			// new bucket is opening now
			bucketID = r.bucketID
			l2entries[bucketID].bucketID = bucketID
			l2entries[bucketID].dataStart = offset
			l2entries[bucketID].firstRecord = i
			l2entries[bucketID].lastRecord = i + 1
		}
		sz := uint32(0) // size of this particular record
		n, err := writeKey(writer, r.key)
		if err != nil {
			return 0, nil, fmt.Errorf("error in writing a key: %w", err)
		}
		sz += n
		n, err = writeValue(writer, r.value, minExpiry)
		if err != nil {
			return 0, nil, fmt.Errorf("error in writing a value: %w", err)
		}
		sz += n
		offset += uint64(sz)
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

func getRecords(allEntries []Entry, numBuckets uint32) []record {
	bucketEntries := make([][]record, numBuckets)
	for i := range bucketEntries {
		bucketEntries[i] = make([]record, 0, 2*numRecordsPerBucket)
	}
	for _, e := range allEntries {
		hash := Hash(e.key)
		bucketID := getBucketID(hash, numBuckets)
		bucketEntries[bucketID] = append(bucketEntries[bucketID], record{
			bucketID: getBucketID(hash, numBuckets),
			fp:       getFingerprint(hash),
			key:      *(*string)(unsafe.Pointer(&e.key)),
			value:    e.val,
		})
	}
	// sort entries within each bucket by their fingerprint so that we
	// can do early termination in the get path
	for _, entries := range bucketEntries {
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].fp < entries[j].fp
		})
	}
	i := 0
	records := make([]record, len(allEntries))
	for _, entries := range bucketEntries {
		i += copy(records[i:], entries)
	}
	return records
}

func buildHashTable(filepath string, entries []Entry) error {
	f, err := os.Create(filepath)
	if err != nil {
		return err
	}
	numRecords := uint32(len(entries))
	numBuckets := numRecords / numRecordsPerBucket
	if numBuckets == 0 {
		numBuckets = 1
	}

	minExpiry := Timestamp(math2.MaxUint32)
	maxExpiry := Timestamp(0)
	records := getRecords(entries, numBuckets)
	for _, r := range records {
		v := r.value
		if v.expires > 0 {
			if minExpiry > v.expires {
				minExpiry = v.expires
			}
			if maxExpiry < v.expires {
				maxExpiry = v.expires
			}
		}
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
	return head, nil
}

func writeKey(writer *bufio.Writer, k string) (uint32, error) {
	n1, err := writeUvarint(writer, uint64(len(k)))
	if err != nil {
		return 0, err
	}
	n2, err := writer.WriteString(k)
	if err != nil {
		return 0, err
	}
	return n1 + uint32(n2), nil
}

// readValue reads a value and returns the number of bytes taken by this value
// if onlysize is true, the caller is only interested in the size of this value
// so this function can avoid expensive operations in those cases
func readValue(data []byte, minExpiry Timestamp, onlysize bool) (Value, int, error) {
	if len(data) == 0 {
		return Value{}, 0, incompleteFile
	}
	if data[0] > 0 { // deleted
		return Value{data: []byte{}, expires: 0, deleted: true}, 1, nil
	}
	cur := 1
	expiry64, n, err := fbinary.ReadUvarint(data[cur:])
	cur += n
	if err != nil {
		return Value{}, 0, err
	}
	expiry := Timestamp(expiry64)
	if expiry > 0 {
		expiry = minExpiry + expiry - 1
	}
	valLen, n, err := fbinary.ReadUvarint(data[cur:])
	cur += n
	if err != nil {
		return Value{}, 0, incompleteFile
	}
	// if expiry > uint32(time.Now().Unix()) { // item has expired
	// 	return Value{
	// 		data:    []byte{},
	// 		expires: Timestamp(expiry),
	// 		deleted: false,
	// 	}, cur + uint32(valLen), nil
	// }
	if onlysize {
		return Value{}, cur + int(valLen), nil
	}
	value := data[cur : cur+int(valLen)]
	return Value{
		data:    clonebytes(value),
		expires: expiry,
		deleted: false,
	}, cur + int(valLen), nil
}

func writeValue(writer *bufio.Writer, v Value, minExpiry Timestamp) (uint32, error) {
	// if value is deleted, it is represented by a single byte of deletion tombstone
	if v.deleted {
		if err := writer.WriteByte(1); err != nil {
			return 0, err
		}
		return 1, nil
	}
	total := uint32(0)
	err := writer.WriteByte(0)
	if err != nil {
		return 0, err
	}
	total += 1
	// if item doesn't expire, its expiry is represented by a single byte of zero
	// else we do varint encoding of (expiry - minExpiry + 1)
	if v.expires == 0 {
		if err := writer.WriteByte(0); err != nil {
			return 0, err
		}
		total += 1
	} else {
		n, err := writeUvarint(writer, uint64(v.expires-minExpiry+1))
		if err != nil {
			return 0, err
		}
		total += n
	}
	// finally write the length of the value and the actual value
	n, err := writeUvarint(writer, uint64(len(v.data)))
	if err != nil {
		return 0, err
	}
	total += n
	vl, err := writer.Write(v.data)
	if err != nil {
		return 0, err
	}
	return total + uint32(vl), nil
}

// take the highest order 24 bits
func getFingerprint(h uint64) fingerprint {
	return fingerprint(h >> (64 - 16))
}

func writeUvarint(buf *bufio.Writer, x uint64) (uint32, error) {
	i := uint32(0)
	for x >= 0x80 {
		if err := buf.WriteByte(byte(x) | 0x80); err != nil {
			return 0, err
		}
		x >>= 7
		i += 1
	}
	return i + 1, buf.WriteByte(byte(x))
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

	overflow := index[int(header.numBuckets)*bucketSizeBytes:]
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

	tableObj := &hashTable{
		name:       name,
		head:       header,
		overflow:   overflow,
		mappedData: buf,
		index:      index,
		data:       data,
		size:       uint64(size),
	}
	runtime.SetFinalizer(tableObj, (*hashTable).Close)
	return tableObj, nil
}

func getBucketID(hash uint64, numBuckets uint32) uint32 {
	return uint32(hash % uint64(numBuckets))
}
