package mem

import (
	"context"
	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/OneOfOne/xxhash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var statsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hangar_mem_stats",
	Help: "Stats about the hanger MemDB",
}, []string{"metric"})

type memDBValueItem struct {
	value        []byte
	expEpochSecs int64
}

type memDBShard struct {
	data    map[string]memDBValueItem
	lock    sync.RWMutex
	rawSize uint64
}

type MemDB struct {
	enc      hangar.Encoder
	planeID  ftypes.RealmID
	shards   []memDBShard
	shardNum uint32

	closeCh chan int
	closeWg sync.WaitGroup
}

func (m *MemDB) StartCompaction() error {
	//TODO implement me
	panic("implement me")
}

func (m *MemDB) StopCompaction() error {
	//TODO implement me
	panic("implement me")
}

func (m *MemDB) Flush() error {
	//TODO implement me
	panic("implement me")
}

func (m *MemDB) Restore(_ io.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (m *memDBShard) Len() int {
	m.lock.RLock()
	defer m.lock.RUnlock()
	return len(m.data)
}

func (m *memDBShard) RawSize() uint64 {
	return atomic.LoadUint64(&m.rawSize)
}

func (m *memDBShard) Get(key string) ([]byte, bool) {
	m.lock.RLock()
	defer m.lock.RUnlock()
	now := time.Now().Unix()
	ret, ok := m.data[key]
	if !ok || (ret.expEpochSecs != 0 && ret.expEpochSecs < now) {
		return nil, false
	}

	newBuf := make([]byte, len(ret.value))
	copy(newBuf, ret.value)
	return newBuf, ok
}

func (m *memDBShard) Del(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	item, ok := m.data[key]
	if ok {
		m.rawSize -= uint64(len(item.value) + len(key))
		delete(m.data, key)
	}
}

func (m *memDBShard) SetWithTTL(key string, value []byte, ttl time.Duration) {
	m.lock.Lock()
	defer m.lock.Unlock()
	var expEpochSecs int64 = 0
	if ttl != 0 {
		expEpochSecs = time.Now().Add(ttl).Unix()
	}
	prevValue, exist := m.data[key]
	if exist {
		m.rawSize -= uint64(len(prevValue.value) + len(key))
		delete(m.data, key)
	}
	m.data[key] = memDBValueItem{
		value:        value,
		expEpochSecs: expEpochSecs,
	}
	m.rawSize += uint64(len(value) + len(key))
}

func (m *memDBShard) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data = make(map[string]memDBValueItem)
	m.rawSize = 0
}

func NewHangar(planeId ftypes.RealmID, shardNum int, enc hangar.Encoder) (*MemDB, error) {
	ret := MemDB{
		planeID:  planeId,
		enc:      enc,
		shards:   nil,
		shardNum: uint32(shardNum),
		closeCh:  make(chan int),
	}
	for i := 0; i < shardNum; i++ {
		ret.shards = append(ret.shards, memDBShard{data: make(map[string]memDBValueItem), rawSize: 0})
	}
	ret.startReportStats()
	return &ret, nil
}

func (m *MemDB) startReportStats() {
	m.closeWg.Add(1)
	go func() {
		interval := time.Second * 10
		t := time.NewTimer(interval)
		defer m.closeWg.Done()
		defer t.Stop()
		for {
			select {
			case _, ok := <-m.closeCh:
				if !ok {
					zap.L().Info("report stats goroutine got closing signal, returning...")
					return
				}
			case <-t.C:
				statsGauge.WithLabelValues("total_items").Set(float64(m.Len()))
				statsGauge.WithLabelValues("total_raw_data_size").Set(float64(m.RawTotalSize()))
				t.Reset(interval)
			}
		}
	}()
}

func (m *MemDB) Len() int {
	ret := 0
	for i := 0; i < int(m.shardNum); i++ {
		ret += m.shards[i].Len()
	}
	return ret
}

func (m *MemDB) RawTotalSize() uint64 {
	var ret uint64 = 0
	for i := 0; i < int(m.shardNum); i++ {
		ret += m.shards[i].RawSize()
	}
	return ret
}

func (m *MemDB) PlaneID() ftypes.RealmID {
	return m.planeID
}

func (m *MemDB) keyToShardIDAndString(key []byte) (uint32, string) {
	sKey := string(key[:])
	return xxhash.Checksum32(key) % m.shardNum, sKey
}

func (m *MemDB) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	_, t := timer.Start(ctx, m.planeID, "hangar.mem.getmany")
	defer t.Stop()
	// We try to spread across available workers while giving each worker
	// a minimum of CACHE_BATCH_SIZE keyGroups to work on.
	eks, err := hangar.EncodeKeyManyKG(kgs, m.enc)
	if err != nil {
		return nil, fmt.Errorf("error encoding key: %w", err)
	}

	vgs := make([]hangar.ValGroup, len(kgs))
	for i, ek := range eks {
		shardID, sKey := m.keyToShardIDAndString(ek)
		valBytes, found := m.shards[shardID].Get(sKey)
		if !found {
			// not found, this is not an error, so we will just return empty ValGroup
			continue
		}
		if _, err := m.enc.DecodeVal(valBytes, &vgs[i], true); err != nil {
			return nil, fmt.Errorf("error decoding value: %w", err)
		} else {
			if kgs[i].Fields.IsPresent() {
				vgs[i].Select(kgs[i].Fields.MustGet())
			}
		}
	}
	return vgs, nil
}

func (m *MemDB) SetMany(ctx context.Context, keys []hangar.Key, deltas []hangar.ValGroup) error {
	_, t := timer.Start(ctx, m.planeID, "hangar.mem.setmany")
	defer t.Stop()
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
	}
	// Consolidate updates to fields in the same key.
	keys, deltas, err := hangar.MergeUpdates(keys, deltas)
	if err != nil {
		return fmt.Errorf("failed to merge updates: %w", err)
	}
	// since we may only be setting some indices of the keyGroups, we need to
	// read existing deltas, merge them, and get the full deltas to be written
	eks, err := hangar.EncodeKeyMany(keys, m.enc)
	if err != nil {
		return nil
	}
	for i, ek := range eks {
		var oldvg hangar.ValGroup

		shardID, sKey := m.keyToShardIDAndString(ek)
		valBytes, found := m.shards[shardID].Get(sKey)
		if found {
			if _, err := m.enc.DecodeVal(valBytes, &oldvg, true); err != nil {
				return err
			}
			if err := oldvg.Update(deltas[i]); err != nil {
				return err
			}
		} else {
			oldvg = deltas[i]
		}
		deltas[i] = oldvg
	}
	return m.commit(eks, deltas, nil)
}

func (m *MemDB) DelMany(ctx context.Context, keyGroups []hangar.KeyGroup) error {
	_, t := timer.Start(ctx, m.planeID, "hangar.mem.delmany")
	defer t.Stop()
	eks, err := hangar.EncodeKeyManyKG(keyGroups, m.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]hangar.ValGroup, 0, len(keyGroups))
	delKeys := make([][]byte, 0, len(keyGroups))
	for i, ek := range eks {
		shardID, sKey := m.keyToShardIDAndString(ek)
		valBytes, found := m.shards[shardID].Get(sKey)
		if !found {
			// nothing to delete
			continue
		}
		var asvg hangar.ValGroup
		if _, err := m.enc.DecodeVal(valBytes, &asvg, true); err != nil {
			return err
		}
		if keyGroups[i].Fields.IsAbsent() {
			delKeys = append(delKeys, ek)
		} else {
			asvg.Del(keyGroups[i].Fields.MustGet())
			if len(asvg.Fields) > 0 {
				setKeys = append(setKeys, ek)
				vgs = append(vgs, asvg)
			} else {
				delKeys = append(delKeys, ek)
			}
		}
	}
	return m.commit(setKeys, vgs, delKeys)
}

func (m *MemDB) commit(eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) error {
	// now we have all the deltas, we can set them
	var valBufs [][]byte = nil

	for i := 0; i < len(eks); i++ {
		_, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			valBufs = append(valBufs, nil)
		} else {
			buf := make([]byte, m.enc.ValLenHint(vgs[i]))
			n, err := m.enc.EncodeVal(buf, vgs[i])
			if err != nil {
				return err
			}
			buf = buf[:n]
			valBufs = append(valBufs, buf)
		}
	}

	for i, ek := range eks {
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		shardID, sKey := m.keyToShardIDAndString(ek)
		if !alive {
			m.shards[shardID].Del(sKey)
		} else {
			m.shards[shardID].SetWithTTL(sKey, valBufs[i], ttl)
		}
	}
	for _, k := range delks {
		shardID, sKey := m.keyToShardIDAndString(k)
		m.shards[shardID].Del(sKey)
	}
	return nil
}

func (m *MemDB) Teardown() error {
	return m.Close()
}

func (m *MemDB) Close() error {
	var i uint32
	close(m.closeCh)
	for i = 0; i < m.shardNum; i++ {
		m.shards[i].Clear()
	}
	m.closeWg.Wait()
	return nil
}

func (m *MemDB) Backup(_ io.Writer, _ uint64) (uint64, error) {
	return 0, nil
}

var _ hangar.Hangar = (*MemDB)(nil)
