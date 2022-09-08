package gravel

import (
	"sync"
)

type Memtable struct {
	lock sync.RWMutex
	map_ map[string]Value
	size uint64
}

func NewMemTable() Memtable {
	return Memtable{
		map_: make(map[string]Value),
	}
}

func (mt *Memtable) Get(k []byte) (Value, error) {
	mt.lock.RLock()
	val, ok := mt.map_[string(k)]
	mt.lock.RUnlock()
	if !ok {
		return Value{}, ErrNotFound
	} else {
		return val, nil
	}
}

func (mt *Memtable) Iter() map[string]Value {
	return mt.map_
}

// Size returns the total size of keys/values as they will be written in the table
// Note that the return of this function may be smaller than the actual memory footprint
// of this memtable
func (mt *Memtable) Size() uint64 {
	return mt.size
}

func (mt *Memtable) Len() uint64 {
	mt.lock.RLock()
	ret := uint64(len(mt.map_))
	mt.lock.RUnlock()
	return ret
}

func (mt *Memtable) SetMany(entries []Entry) error {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	for _, e := range entries {
		mt.map_[string(e.key)] = e.val
		mt.size += uint64(sizeof(e))
	}
	return nil
}

func (mt *Memtable) Clear() error {
	mt.lock.Lock()
	defer mt.lock.Unlock()

	for k := range mt.map_ {
		delete(mt.map_, k)
	}
	mt.size = 0
	return nil
}

// Flush flushes the memtable to the disk
// Note - it doesn't yet clear the memtable (and so continues serving writes) until
// explicitly called after the table has been added to the table list
func (mt *Memtable) Flush(type_ TableType, dirname string, id uint64) (Table, error) {
	mt.lock.Lock()
	defer mt.lock.Unlock()
	return BuildTable(dirname, id, type_, mt)
}
