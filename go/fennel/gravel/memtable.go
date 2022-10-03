package gravel

import (
	"sync"
	"unsafe"
)

type Memtable struct {
	numShards  uint64
	writelock  *sync.RWMutex
	shardLocks []sync.RWMutex
	maps       []map[string]Value
	size       uint64
	len        uint64
}

func NewMemTable(numShards uint64) Memtable {
	maps := make([]map[string]Value, numShards)
	locks := make([]sync.RWMutex, numShards)
	for i := 0; i < int(numShards); i++ {
		maps[i] = make(map[string]Value)
		locks[i] = sync.RWMutex{}
	}
	return Memtable{
		numShards:  numShards,
		writelock:  &sync.RWMutex{},
		shardLocks: locks,
		maps:       maps,
		size:       0,
	}
}

func (mt *Memtable) Get(k []byte, shard uint64) (Value, error) {
	lock := &mt.shardLocks[shard]
	lock.RLock()
	val, ok := mt.maps[shard][string(k)]
	lock.RUnlock()
	if !ok {
		return Value{}, ErrNotFound
	} else {
		return val, nil
	}
}

func (mt *Memtable) Iter(shard uint64) map[string]Value {
	return mt.maps[uint(shard)]
}

// Size returns the total size of keys/values as they will be written in the table
// Note that the return of this function may be smaller than the actual memory footprint
// of this memtable
func (mt *Memtable) Size() uint64 {
	return mt.size
}

func (mt *Memtable) Len() uint64 {
	mt.writelock.RLock()
	ret := mt.len
	mt.writelock.RUnlock()
	return ret
}

func (mt *Memtable) SetMany(entries []Entry, stats *Stats) error {
	mt.writelock.Lock()
	defer mt.writelock.Unlock()
	for _, e := range entries {
		hash := Hash(e.key)
		shard := Shard(hash, mt.numShards)
		map_ := mt.maps[shard]
		mt.shardLocks[shard].Lock()
		// keys/values of entries are owned by gravel (because we clone this data)
		// and so this data isn't going to be modified by anyone ever. Given this,
		// we can safely typecast []byte to string without worries of modification
		// and save one allocation
		s := *(*string)(unsafe.Pointer(&e.key))
		if v, found := map_[s]; found {
			mt.size -= uint64(sizeof(Entry{
				key: e.key,
				val: v,
			}))
			mt.len -= 1
		}
		map_[s] = e.val
		mt.size += uint64(sizeof(e))
		mt.len += 1
		if e.val.deleted {
			maybeInc(shouldSample(), &stats.Dels)
		} else {
			maybeInc(shouldSample(), &stats.Sets)
		}
		mt.shardLocks[shard].Unlock()
	}
	stats.MemtableSizeBytes.Store(mt.Size())
	stats.MemtableKeys.Store(mt.len)
	maybeInc(shouldSample(), &stats.Commits)
	return nil
}

func (mt *Memtable) Clear() error {
	mt.writelock.Lock()
	defer mt.writelock.Unlock()

	for _, m := range mt.maps {
		// erase by deletion instead of creating a new map, only to reduce GC burden
		// downside is blocking the read due to locking.
		// TODO: shadow memtable to avoid long lock holding
		for k := range m {
			delete(m, k)
		}
	}

	mt.size = 0
	mt.len = 0
	return nil
}

// Flush flushes the memtable to the disk
// Note - it doesn't yet clear the memtable (and so continues serving writes) until
// explicitly called after the table has been added to the table list
func (mt *Memtable) Flush(type_ TableType, dirname string) ([]string, error) {
	mt.writelock.RLock()
	defer mt.writelock.RUnlock()
	return BuildTable(dirname, mt.numShards, type_, mt)
}
