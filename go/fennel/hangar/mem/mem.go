package mem

import (
	"context"
	"encoding/binary"
	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var statsGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
	Name: "hangar_mem_stats",
	Help: "Stats about the hanger MemDB",
}, []string{"metric"})

const dataFileSuffix = ".memdb"
const dataTmpFileSuffix = ".memdb.tmp"

type memDBValueItem struct {
	value        []byte
	expEpochSecs int64
}

type memDBItem struct {
	key   string
	value memDBValueItem
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
	path     string

	closeCh chan int
	closeWg sync.WaitGroup
}

func (m *MemDB) Restore(_ io.Reader) error {
	//TODO implement me
	panic("implement me")
}

func (m *memDBShard) Items() int {
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

func (m *memDBShard) DumpAndCleanup(writer io.Writer) error {
	now := time.Now().Unix()
	m.lock.Lock()
	// Clean up expired objects
	for key, value := range m.data {
		if (value.expEpochSecs > 0) && (now > value.expEpochSecs) {
			itemSize := len(value.value) + len(key)
			delete(m.data, key)
			m.rawSize -= uint64(itemSize)
		}
	}
	m.lock.Unlock()

	m.lock.RLock()
	defer m.lock.RUnlock()
	// now dump into file
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(len(m.data)))
	_, err := writer.Write(buf)
	if err != nil {
		return err
	}
	for key, value := range m.data {
		buf := make([]byte, 16)
		binary.BigEndian.PutUint32(buf, uint32(len(key)))
		binary.BigEndian.PutUint32(buf[4:], uint32(len(value.value)))
		binary.BigEndian.PutUint64(buf[8:], uint64(value.expEpochSecs))
		_, err = writer.Write(buf)
		if err != nil {
			return err
		}
		_, err = writer.Write([]byte(key))
		if err != nil {
			return err
		}
		_, err = writer.Write(value.value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *memDBShard) StartLoadGoroutine(ch chan *memDBItem, wg *sync.WaitGroup) {
	go func() {
		defer wg.Done()
		m.Clear()
		m.lock.Lock()
		defer m.lock.Unlock()
		for item := range ch {
			m.data[item.key] = item.value
			m.rawSize += uint64(len(item.key) + len(item.value.value))
		}
	}()
}

func (m *memDBShard) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()
	m.data = make(map[string]memDBValueItem)
	m.rawSize = 0
}

func NewHangar(planeId ftypes.RealmID, shardNum int, path string, enc hangar.Encoder) (*MemDB, error) {
	ret := MemDB{
		planeID:  planeId,
		enc:      enc,
		shards:   nil,
		shardNum: uint32(shardNum),
		path:     path,
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
				statsGauge.WithLabelValues("total_items").Set(float64(m.Items()))
				statsGauge.WithLabelValues("total_raw_data_size").Set(float64(m.RawTotalSize()))
				t.Reset(interval)
			}
		}
	}()
}

func (m *MemDB) Items() int {
	ret := 0
	for i := 0; i < int(m.shardNum); i++ {
		ret += m.shards[i].Items()
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

func (m *MemDB) Load() error {
	var fileNames []string = nil
	files, err := ioutil.ReadDir(m.path)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), dataFileSuffix) {
			fileNames = append(fileNames, filepath.Join(m.path, f.Name()))
		}
	}

	if (err != nil) || (len(fileNames) == 0) {
		return nil
	}

	const ioGoroutineNum = 16
	wgIOGoroutine := sync.WaitGroup{}
	wgShardGoroutine := sync.WaitGroup{}

	fileCh := make(chan string, 4)
	var ret error = nil

	var itemChs []chan *memDBItem = nil
	for i := 0; i < int(m.shardNum); i++ {
		itemChs = append(itemChs, make(chan *memDBItem, 256))
	}

	for i := 0; i < ioGoroutineNum; i++ {
		wgIOGoroutine.Add(1)
		go func() {
			defer wgIOGoroutine.Done()
			for fileName := range fileCh {
				f, err := os.Open(fileName)
				if err != nil {
					zap.L().Error("Failed to open file", zap.String("filename", fileName), zap.Error(err))
					return
				}
				for {
					buf := make([]byte, 8)
					bytesRead, err := f.Read(buf)
					if bytesRead != 8 {
						if err == io.EOF {
							zap.L().Info("Finished reading data file", zap.String("filename", fileName))
						} else {
							zap.L().Error("Failed to open file", zap.String("filename", fileName), zap.Error(err))
							ret = err
						}
						break
					}
					itemCount := binary.BigEndian.Uint64(buf)
					var idx uint64
					for idx = 0; idx < itemCount; idx++ {
						buf := make([]byte, 16)
						bytesRead, err := f.Read(buf)
						if bytesRead != 16 {
							zap.L().Error("Can't read enough bytes", zap.String("filename", fileName), zap.Error(err))
							ret = fmt.Errorf("encountered incomplete file %s", fileName)
							break
						}
						keyLen := int(binary.BigEndian.Uint32(buf))
						valueLen := int(binary.BigEndian.Uint32(buf[4:]))
						expEpochSecs := int64(binary.BigEndian.Uint64(buf[8:]))

						keyBuf := make([]byte, keyLen)
						bytesRead, err = f.Read(keyBuf)
						if bytesRead != keyLen {
							zap.L().Error("Can't read enough bytes", zap.String("filename", fileName), zap.Error(err))
							ret = fmt.Errorf("encountered incomplete file %s", fileName)
							break
						}
						valueBuf := make([]byte, valueLen)
						bytesRead, err = f.Read(valueBuf)
						if bytesRead != valueLen {
							zap.L().Error("Can't read enough bytes", zap.String("filename", fileName), zap.Error(err))
							ret = fmt.Errorf("encountered incomplete file %s", fileName)
							break
						}
						shardID, sKey := m.keyToShardIDAndString(keyBuf)
						itemChs[shardID] <- &memDBItem{key: sKey, value: memDBValueItem{value: valueBuf, expEpochSecs: expEpochSecs}}
					}
				}
				_ = f.Close()
			}
		}()
	}

	for i := 0; i < int(m.shardNum); i++ {
		wgShardGoroutine.Add(1)
		m.shards[i].StartLoadGoroutine(itemChs[i], &wgShardGoroutine)
	}

	for _, fileName := range fileNames {
		fileCh <- fileName
	}
	close(fileCh)
	wgIOGoroutine.Wait()
	for i := 0; i < int(m.shardNum); i++ {
		close(itemChs[i])
	}
	wgShardGoroutine.Wait()
	return ret
}

func (m *MemDB) Save() error {
	files, _ := ioutil.ReadDir(m.path)
	for _, f := range files {
		fullName := filepath.Join(m.path, f.Name())
		if strings.HasSuffix(f.Name(), dataTmpFileSuffix) {
			err := os.Remove(fullName)
			if err != nil {
				return fmt.Errorf("failed to remove previous temp file %s: %w", fullName, err)
			}
		}
	}

	const ioGoroutineNum = 16
	wg := sync.WaitGroup{}
	var fileIdx int64 = 0
	var ret error = nil

	shardCh := make(chan int, 4)
	for i := 0; i < ioGoroutineNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var currentFile *os.File = nil
			shardInFile := 0
			for shardId := range shardCh {
				if currentFile == nil {
					// open a new file
					var err error
					shardInFile = 0
					currentFileIdx := atomic.AddInt64(&fileIdx, 1)
					fileName := filepath.Join(m.path, fmt.Sprintf("datashard-%d%s", currentFileIdx, dataTmpFileSuffix))
					currentFile, err = os.Create(fileName)
					if err != nil {
						zap.L().Error("Failed to create file to dump", zap.String("filename", fileName), zap.Error(err))
						ret = err
						break
					}
				}
				err := m.shards[shardId].DumpAndCleanup(currentFile)
				if err != nil {
					zap.L().Error("Failed to dump the dataset of shard", zap.Int("shard_id", shardId), zap.Error(err))
					ret = err
					break
				}
				zap.L().Info("Successfully dumped the dataset of shard", zap.Int("shard_id", shardId))
				shardInFile += 1
				if shardInFile == 4 {
					_ = currentFile.Close()
					currentFile = nil
				}
			}
			if currentFile != nil {
				_ = currentFile.Close()
				currentFile = nil
			}
		}()
	}

	for i := 0; i < int(m.shardNum); i++ {
		shardCh <- i
	}
	close(shardCh)
	wg.Wait()
	if ret == nil {
		files, _ := ioutil.ReadDir(m.path)
		for _, f := range files {
			fullName := filepath.Join(m.path, f.Name())
			if strings.HasSuffix(f.Name(), dataFileSuffix) {
				err := os.Remove(fullName)
				if err != nil {
					return fmt.Errorf("failed to remove previous temp file %s: %w", fullName, err)
				}
			}
		}
		files, _ = ioutil.ReadDir(m.path)
		for _, f := range files {
			fullName := filepath.Join(m.path, f.Name())
			if strings.HasSuffix(f.Name(), dataTmpFileSuffix) {
				newFullName := strings.TrimSuffix(fullName, ".tmp")
				err := os.Rename(fullName, newFullName)
				if err != nil {
					return fmt.Errorf("failed to rename file from %s to %s: %w", fullName, newFullName, err)
				}
			}
		}
		zap.L().Info("Saved all data successfully")
	}
	return ret
}

func (m *MemDB) PlaneID() ftypes.RealmID {
	return m.planeID
}

func (m *MemDB) keyToShardIDAndString(key []byte) (uint32, string) {
	sKey := string(key[:])
	return crc32.ChecksumIEEE(key) % m.shardNum, sKey
}

func (m *MemDB) SimpleSet(key []byte, value []byte, ttl time.Duration) {
	shardID, sKey := m.keyToShardIDAndString(key)
	m.shards[shardID].SetWithTTL(sKey, value, ttl)
}

func (m *MemDB) SimpleGet(key []byte) ([]byte, bool) {
	shardID, sKey := m.keyToShardIDAndString(key)
	val, ok := m.shards[shardID].Get(sKey)
	return val, ok
}

func (m *MemDB) SimpleDel(key []byte) {
	shardID, sKey := m.keyToShardIDAndString(key)
	m.shards[shardID].Del(sKey)
}

func (m *MemDB) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	ctx, t := timer.Start(ctx, m.planeID, "hangar.mem.getmany")
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
	for i, ek := range eks {
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			shardID, sKey := m.keyToShardIDAndString(ek)
			m.shards[shardID].Del(sKey)
		} else {
			buf := make([]byte, m.enc.ValLenHint(vgs[i]))
			n, err := m.enc.EncodeVal(buf, vgs[i])
			if err != nil {
				return err
			}
			buf = buf[:n]

			shardID, sKey := m.keyToShardIDAndString(ek)
			m.shards[shardID].SetWithTTL(sKey, buf, ttl)
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
