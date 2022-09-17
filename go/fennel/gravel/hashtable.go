package gravel

/*
	This file implements a disk based hash table optimized for immutable files.
	It optimizes for the case where keys are not present in the table - in such
	cases, it only takes one index read (and two in a tiny fraction of requests).
	The index is small enough that we can keep the entire index in RAM.
	As a result, the common case of absent key is blazing fast. When the key is
	present, besides 1-2 RAM lookups, it takes upto one disk read (and even that
	may be avoided if the data section is cached in RAM).

	Contents of hashtable are mmaped and so if the machine has enough RAM, ALL the
	lookups can be served via memory only.

	Here is how the table is laid out in disk
		Header (64 bytes)
		Data (variable size, aligned with 64 bytes)
		L1 Index (variable size - fixed 64 bytes for each hash bucket)
		L2 Index (variable size - single block for all index data that doesn't fit in L1 index)

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
		4 bytes shard ID
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
		Optional: 4 byte offset in L2 index of the overflow fingerprints - this is present only
			if the number of keys in the bucket could not have fit in 64 bytes
		2 bytes fingerprint for each of the keys in this bucket

	2nd level index stores any remaining fingerprints that could not fit within 64 bytes of L1 entry.
	Each fingerprint is fixed 2 bytes.

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
	"encoding/binary"
	"errors"
	"fennel/lib/utils"
	fbinary "fennel/lib/utils/binary"
	"fennel/lib/utils/math"
	"fennel/lib/utils/slice"
	"fmt"
	math2 "math"
	"os"
	"path"
	"runtime"
	"sort"
	"syscall"

	"go.uber.org/atomic"
	"golang.org/x/sys/unix"
)

const (
	emptyBucket         uint32 = 0xFFFFFFFF
	magicHeader         uint32 = 0x24112021 // this is just an arbitrary 32 bit number - day Fennel was incorporated :)
	v1codec_xxhash      uint8  = 1
	numRecordsPerBucket uint32 = 8
)

type fingerprint uint16

// this should be 64 bytes and fit in a single cache line
type header struct {
	moduloMask  uint64 // note: this entry isn't written to disk but is part of memory struct
	datasize    uint64
	indexsize   uint32
	numRecords  uint32
	numBuckets  uint32
	minExpiry   Timestamp
	maxExpiry   Timestamp
	magic       uint32
	shardbits   uint8
	codec       uint8
	encrypted   bool
	compression uint8
	_           [16]byte
}

// record denotes a k-v pair that exists within the table
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
	datastart   uint64
	firstRecord uint32
	lastrecord  uint32
}

type hashTable struct {
	head     header
	index    []byte
	overflow []byte
	data     []byte
	id       uint64
	reads    atomic.Uint64
}

func (ht *hashTable) Get(key []byte, hash uint64) (Value, error) {
	head := ht.head // deref in local variables to reduce address translations
	bucketID := getBucketID(hash, head.moduloMask, head.shardbits)
	fp := getFingerprint(hash)

	numKeys, datapos, err := ht.readIndex(bucketID, uint16(fp))
	if err != nil {
		return Value{}, err
	}
	ret, err := ht.readData(datapos, numKeys, key, uint32(head.minExpiry))
	return ret, err
}

// readIndex reads the index for bucketID and searches for the given fingerprint.
// If the fingerprint is found, it returns the number of keys to scan in the data section,
// the offset within data section where scanning should begin, and the error if any
// If the key isn't found, err is set to ErrNotFound
func (ht *hashTable) readIndex(bucketID uint32, fp uint16) (uint32, uint64, error) {
	indexstart := bucketID << 6
	indexend := indexstart + 64
	index := ht.index[indexstart:indexend]
	if len(index) != 64 {
		return 0, 0, fmt.Errorf("index entry is not of size 64 bytes")
	}
	numKeys := index[0]
	datapos := uint64(index[1])<<32 + uint64(index[2])<<24 + uint64(index[3])<<16 + uint64(index[4])<<8 + uint64(index[5])
	var overflow uint32
	sofar := 6
	// if all keys don't fit, read overflow offset
	if numKeys > 29 { // equivalent to 2 * numKeys > 64 - 1 - 5
		overflow = binary.LittleEndian.Uint32(index[5:9])
		sofar += 4
	}
	// now read all fps one by one until one matches or we run out of them
	curkey := uint8(0)
	for ; curkey < numKeys && curkey < 30; curkey += 1 {
		curfp := binary.LittleEndian.Uint16(index[sofar:])
		sofar += 2
		if curfp > fp {
			// we keep fingerprints sorted so if this one is bigger, all subsequent ones are too
			return 0, 0, ErrNotFound
		}
		if curfp == fp {
			return uint32(numKeys), datapos, nil
		}
	}
	if curkey >= numKeys {
		return 0, 0, ErrNotFound
	}
	// if there were any in the overflow section, check that too
	l2index := ht.overflow
	for ; curkey < numKeys; curkey += 1 {
		if overflow > uint32(len(l2index)) {
			return 0, 0, incompleteFile
		}
		curfp := binary.LittleEndian.Uint16(l2index[overflow:])
		if curfp == fp {
			return uint32(numKeys), datapos, nil
		}
		overflow += 2
	}
	return 0, 0, ErrNotFound
}

// writeIndex writes all the index data via writer and returns the number of bytes
// written and the error if any. This assumes that both l2entries and records are
// sorted on bucketID
func writeIndex(writer *bufio.Writer, numBuckets uint32, l2entries []bucket, records []record) (uint32, error) {
	overflow := make([]byte, 0, 1<<24) // creating an arbitrary large buffer on stack for copying
	buf := make([]byte, 8)
	entry := make([]byte, 64)
	for bid, l2idx := uint32(0), 0; bid < numBuckets; bid++ {
		slice.Fill(entry, 0)
		// this bucket has no corresponding bucket => it's an empty bucket
		if l2idx >= len(l2entries) || l2entries[l2idx].bucketID != bid {
			if _, err := writer.Write(entry); err != nil {
				return 0, err
			}
			continue
		}
		l2entry := l2entries[l2idx]
		// fmt.Printf("going to create non zero l2 entry using: %+v\n", bucket)
		l2idx += 1
		numKeys := l2entry.lastrecord - l2entry.firstRecord
		if numKeys > 256 {
			return 0, fmt.Errorf("number of keys in the bucket greater than 256: %d", numKeys)
		}
		// first write the number of keys in this bucket
		entry[0] = uint8(numKeys)
		// then write the position of this bucket's data (as offset within data segment) in 5 bytes
		datapos := l2entry.datastart
		if datapos >= (1 << 40) {
			return 0, fmt.Errorf("value too large, doesn't fit in 5 bytes")
		}
		entry[1] = uint8(datapos>>32) & 255
		entry[2] = uint8(datapos>>24) & 255
		entry[3] = uint8(datapos>>16) & 255
		entry[4] = uint8(datapos>>8) & 255
		entry[5] = uint8(datapos & 255)

		sofar := 6
		// if all 2 byte fingerprints don't in the 64 bytes, set the
		// overflow offset here
		if numKeys > 29 { // equivalent to 2 * numKeys > 64 - 6
			binary.LittleEndian.PutUint32(entry[sofar:], uint32(len(overflow)))
			sofar += 4
		}
		// now write as many fingerprints as we can within the rest of 64 bytes
		curRecord := l2entry.firstRecord
		for ; curRecord < l2entry.lastrecord && sofar+2 <= 64; curRecord += 1 {
			binary.LittleEndian.PutUint16(entry[sofar:], uint16(records[curRecord].fp))
			sofar += 2
		}
		// and if any were left, write them to the overflow section
		for ; curRecord < l2entry.lastrecord; curRecord += 1 {
			binary.LittleEndian.PutUint16(buf, uint16(records[curRecord].fp))
			overflow = append(overflow, buf[0], buf[1])
			sofar += 2
		}
		// now write this entry to the writer
		if _, err := writer.Write(entry); err != nil {
			return 0, err
		}
	}
	// now write all the overflow section
	// fmt.Printf("overflow section was: %d bytes\n", len(overflow))
	if _, err := writer.Write(overflow); err != nil {
		return 0, err
	}
	return 64*numBuckets + uint32(len(overflow)), nil
}

// readData reads the data segment starting at 'start' and read upto numRecords to find a
// record that matches given key. If found, the value is returned, else err is set to ErrNotFound
func (ht *hashTable) readData(start uint64, numRecords uint32, key []byte, minExpiry uint32) (Value, error) {
	data := ht.data[start:]
	sofar := uint32(0)
	for i := 0; i < int(numRecords); i++ {
		keylen, n, err := fbinary.ReadUvarint(data[sofar:])
		if err != nil {
			return Value{}, incompleteFile
		}
		sofar += uint32(n)
		curkey := data[sofar : sofar+uint32(keylen)]
		sofar += uint32(keylen)
		if bytes.Equal(key, curkey) {
			ret, _, err := readValue(data[sofar:], minExpiry, false)
			return ret, err
		} else {
			_, m, err := readValue(data[sofar:], minExpiry, true)
			if err != nil {
				return Value{}, err
			}
			sofar += m
		}
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
	l2idx := 0            // index in l2entries list that we are observing/updating at any point of time
	offset := uint64(0)
	for i, r := range records {
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
		if i == 0 || r.bucketID == bucketID {
			// bucket continues
			l2entries[l2idx].lastrecord += 1
		} else {
			// new bucket is opening now
			bucketID = r.bucketID
			l2idx += 1
			l2entries[l2idx].bucketID = bucketID
			l2entries[l2idx].datastart = offset
			l2entries[l2idx].firstRecord = uint32(i)
			l2entries[l2idx].lastrecord = uint32(i + 1)
		}
		offset += uint64(sz)
	}
	return offset, l2entries, nil
}

func (ht *hashTable) Close() error {
	if ht.data != nil {
		data := ht.data
		ht.data = nil
		if err := syscall.Munmap(data); err != nil {
			return err
		}
	}
	if ht.index != nil {
		index := ht.index
		ht.index = nil
		if err := syscall.Munmap(index); err != nil {
			return err
		}
	}
	runtime.SetFinalizer(ht, nil)
	return nil
}

func (ht *hashTable) ID() uint64 {
	return ht.id
}

func (ht *hashTable) DataReads() uint64 {
	return ht.reads.Load() * sampleRate
}

var _ Table = (*hashTable)(nil)

func buildHashTable(dirname string, numShards uint64, mt *Memtable) ([]string, error) {
	shardbits, err := log2(numShards)
	if err != nil {
		return nil, err
	}
	filenames := make([]string, 0, uint(numShards))
	for shard := uint64(0); shard < numShards; shard += 1 {
		filename, err := buildShard(shard, shardbits, dirname, mt.Iter(shard))
		if err != nil {
			return nil, err
		}
		filenames = append(filenames, filename)
	}
	return filenames, nil
}

func buildShard(shard uint64, shardbits uint8, dirname string, data map[string]Value) (string, error) {
	filename := fmt.Sprintf("%d_%s%s", shard, utils.RandString(8), tempSuffix)
	filepath := path.Join(dirname, filename)
	f, err := os.Create(filepath)
	if err != nil {
		return "", err
	}
	numRecords := uint32(len(data))
	numBuckets := uint64(numRecords / numRecordsPerBucket)
	numBuckets = math.NextPowerOf2(numBuckets)
	moduloMask := numBuckets - 1 // since numBucket is power of 2, we can & with this mask instead of taking modulo
	records := make([]record, 0, len(data))
	minExpiry := Timestamp(math2.MaxUint32)
	maxExpiry := Timestamp(0)
	for k, v := range data {
		hash := Hash([]byte(k))
		records = append(records, record{
			bucketID: getBucketID(hash, moduloMask, shardbits),
			fp:       getFingerprint(hash),
			key:      k,
			value:    v,
		})
		if v.expires > 0 {
			if minExpiry > v.expires {
				minExpiry = v.expires
			}
			if maxExpiry < v.expires {
				maxExpiry = v.expires
			}
		}
	}
	// sort all the records so that those with the same bucket ID come close together
	// within the same bucket, we sort records by fingerprint. This will allow us to
	// early termination when fingerprints don't match
	sort.Slice(records, func(i, j int) bool {
		ri, rj := &records[i], &records[j]
		return ri.bucketID < rj.bucketID || (ri.bucketID == rj.bucketID && ri.fp < rj.fp)
	})
	// now start writing the data section starting byte 64 (leaving bytes 0 - 63 for header)
	_, err = f.Seek(64, unix.SEEK_SET)
	if err != nil {
		return "", err
	}
	writer := bufio.NewWriterSize(f, 1024*1024)
	datasize, buckets, err := writeData(writer, records, minExpiry, uint32(numBuckets))
	if err != nil {
		return "", err
	}
	// We want index data to be 64 byte aligned, so if data size is not a multiple of 64, add some filler
	gap := 64 - (datasize & 63)
	if _, err = writer.Write(make([]byte, gap)); err != nil {
		return "", fmt.Errorf("failed to write data segment: %w", err)
	}
	datasize += gap
	// now write the index
	indexsize, err := writeIndex(writer, uint32(numBuckets), buckets, records)
	if err != nil {
		return "", fmt.Errorf("failed to write index segment: %w", err)
	}
	// flush any existing data in the writer and then go to the start to write the header
	if err = writer.Flush(); err != nil {
		return "", fmt.Errorf("error flushing the buffered writer %w", err)
	}
	_, err = f.Seek(0, unix.SEEK_SET)
	if err != nil {
		return "", err
	}
	writer.Reset(f)
	head := header{
		magic:       magicHeader,
		codec:       v1codec_xxhash,
		encrypted:   false,
		compression: 0,
		shardbits:   shardbits,
		numRecords:  numRecords,
		numBuckets:  uint32(numBuckets),
		datasize:    datasize,
		indexsize:   indexsize,
		minExpiry:   minExpiry,
		maxExpiry:   maxExpiry,
	}
	if err = writeHeader(writer, head); err != nil {
		return "", err
	}
	if err = writer.Flush(); err != nil {
		return "", fmt.Errorf("error flushing the buffered writer %w", err)
	}
	if err = f.Sync(); err != nil {
		return "", err
	}
	return filename, f.Close()
}

func writeHeader(writer *bufio.Writer, head header) error {
	buf := make([]byte, 64)
	binary.LittleEndian.PutUint32(buf, head.magic)
	buf[4] = head.codec
	// false for encryption
	if head.encrypted {
		buf[5] = 1
	}
	buf[6] = head.compression
	buf[7] = head.shardbits

	binary.LittleEndian.PutUint32(buf[8:12], head.numRecords)
	binary.LittleEndian.PutUint32(buf[12:16], head.numBuckets)
	binary.LittleEndian.PutUint64(buf[16:24], head.datasize)
	binary.LittleEndian.PutUint32(buf[24:28], head.indexsize)
	binary.LittleEndian.PutUint32(buf[28:32], uint32(head.minExpiry))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(head.maxExpiry))
	// fmt.Printf("just before writing, buf was: %v\n", buf)
	if _, err := writer.Write(buf); err != nil {
		return err
	}
	return nil
}

func readHeader(buf []byte) (header, error) {
	if len(buf) != 64 {
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
	head.shardbits = buf[7]
	head.numRecords = binary.LittleEndian.Uint32(buf[8:12])
	head.numBuckets = binary.LittleEndian.Uint32(buf[12:16])
	head.datasize = binary.LittleEndian.Uint64(buf[16:24])
	head.indexsize = binary.LittleEndian.Uint32(buf[24:28])
	head.minExpiry = Timestamp(binary.LittleEndian.Uint32(buf[28:32]))
	head.maxExpiry = Timestamp(binary.LittleEndian.Uint32(buf[32:36]))
	head.moduloMask = uint64(head.numBuckets) - 1 // since numBucket is power of 2, we can & with this mask instead of taking modulo
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
// if onlysize is true, the caller is only interested in the size of thsi value
// so this function can avoid expensive operations in those cases
func readValue(data []byte, minExpiry uint32, onlysize bool) (Value, uint32, error) {
	if len(data) == 0 {
		return Value{}, 0, incompleteFile
	}
	if data[0] > 0 { // deleted
		return Value{data: []byte{}, expires: 0, deleted: true}, 1, nil
	}
	cur := uint32(1)
	expiry64, n, err := fbinary.ReadUvarint(data[cur:])
	cur += uint32(n)
	if err != nil {
		return Value{}, 0, err
	}
	expiry := uint32(expiry64)
	if expiry > 0 {
		expiry = minExpiry + expiry - 1
	}
	valLen, n, err := fbinary.ReadUvarint(data[cur:])
	cur += uint32(n)
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
		return Value{}, cur + uint32(valLen), nil
	}
	value := data[cur : cur+uint32(valLen)]
	return Value{
		data:    clonebytes(value),
		expires: Timestamp(expiry),
		deleted: false,
	}, cur + uint32(valLen), nil
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

func log2(n uint64) (uint8, error) {
	if n&(n-1) > 0 {
		return 0, errors.New("not a power of 2")
	}
	ret := uint8(0)
	for n > 0 {
		ret += 1
		n >>= 1
	}
	return ret, nil
}

// take the highest order 16 bits
func getFingerprint(h uint64) fingerprint {
	return fingerprint((h >> 48) & ((1 << 16) - 1))
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

func openHashTable(id uint64, filepath string) (Table, error) {
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

	// TODO: looks like mmap call takes a size parameter that is of type 'int'
	// does it mean that if the file is bigger than 2GB, are we unable to mmap it all?
	buf, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, err
	}
	// fmt.Printf("header is: %v\n", buf)
	header, err := readHeader(buf[:64])
	if err != nil {
		return nil, fmt.Errorf("error reading header: %w", err)
	}
	datastart := uint64(64)
	dataend := datastart + header.datasize
	data := buf[datastart:dataend]

	indexend := dataend + uint64(header.indexsize)
	index := buf[dataend:indexend]
	overflow := index[header.numBuckets*64:]

	// Prefetch both the index and data by "touching" it
	for i := 0; i < len(index); i++ {
		_ = index[i]
	}
	for i := 0; i < len(data); i++ {
		_ = data[i]
	}

	tableObj := &hashTable{
		head:     header,
		overflow: overflow,
		index:    index,
		data:     data,
		id:       id,
		reads:    atomic.Uint64{},
	}
	runtime.SetFinalizer(tableObj, (*hashTable).Close)
	return tableObj, nil
}

func getBucketID(hash, mask uint64, shardbits uint8) uint32 {
	// ret := uint32((hash >> shardbits) & mask)
	// fmt.Printf("getting bucket id for hash: %d, mask: %d, shardbits %d, ans is: %d\n", hash, mask, shardbits, ret)
	return uint32((hash >> shardbits) & mask)
}
