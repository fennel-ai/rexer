package gravel

import (
	"fmt"
	"go.uber.org/zap"
	"os"
	"path"
	"runtime"
	"sort"
	"sync"
	"time"
)

const (
	StatsTotalSize      = 0
	StatsNumTables      = 1
	StatsTotalReads     = 2
	StatsTotalRecords   = 3
	StatsTotalIndexSize = 4
	// StatsMax should be increased if any new stats inserted here
	StatsMax = 5
)

const (
	compactionPollInterval          = time.Second * 10
	minimumFilesToTriggerCompaction = 8
)

type TableManager struct {
	manifest            *Manifest
	compactionWorkerNum int
	tables              [][]Table
	tablesByFileName    []map[string]Table
	lock                sync.RWMutex
	wg                  sync.WaitGroup
	stopCh              chan struct{}
	compactionRunning   bool
}

func InitTableManager(dirname string, tableType TableType, numShards uint64) (*TableManager, error) {
	if err := numShardsValid(numShards); err != nil {
		return nil, err
	}
	// if the directory doesn't exist, create it
	if err := os.MkdirAll(dirname, os.ModePerm); err != nil {
		return nil, err
	}

	manifest, err := InitManifest(dirname, tableType, numShards)
	if err != nil {
		return nil, err
	}

	tm := &TableManager{
		manifest:          manifest,
		tables:            make([][]Table, manifest.numShards),
		tablesByFileName:  make([]map[string]Table, manifest.numShards),
		lock:              sync.RWMutex{},
		wg:                sync.WaitGroup{},
		compactionRunning: false,
	}

	for i := uint64(0); i < manifest.numShards; i++ {
		err := tm.reloadShardTablesFromManifest(i)
		if err != nil {
			return nil, err
		}
	}

	tm.compactionWorkerNum = runtime.NumCPU() / 5
	if tm.compactionWorkerNum > 8 {
		tm.compactionWorkerNum = 8
	}
	if tm.compactionWorkerNum < 1 {
		tm.compactionWorkerNum = 1
	}

	tm.StartCompactionWorkers()
	return tm, nil
}

// reloadShardTablesFromManifest keeps or opens tables that are in the current manifest, and closes those that are not
// the function should be called with lock protection
func (t *TableManager) reloadShardTablesFromManifest(shardId uint64) error {
	tableFiles, err := t.manifest.GetTableFiles(shardId)
	if err != nil {
		zap.L().Error("failed to read manifest after the compaction", zap.Error(err))
		return err
	}

	var ret error = nil
	tablesByFileName := make(map[string]Table)
	tables := make([]Table, 0, len(tableFiles))
	for _, tableFile := range tableFiles {
		table, exist := t.tablesByFileName[shardId][tableFile]
		if !exist {
			table, err = OpenTable(t.manifest.tableType, path.Join(t.manifest.dirname, tableFile))
			if err != nil {
				zap.L().Error("failed to open table", zap.Error(err))
				ret = err
				continue
			}
			zap.L().Info("opened table", zap.String("tableFile", tableFile), zap.Error(err))
		}
		tablesByFileName[tableFile] = table
		tables = append(tables, table)
	}

	for tableFile, table := range t.tablesByFileName[shardId] {
		if _, ok := tablesByFileName[tableFile]; !ok {
			err = table.Close()
			if err != nil {
				zap.L().Error("failed to close deleted table", zap.Error(err))
				ret = err
				continue
			}
			zap.L().Info("closed table", zap.String("tableFile", tableFile), zap.Error(err))
		}
	}

	t.tables[shardId] = tables
	t.tablesByFileName[shardId] = tablesByFileName
	return ret
}

// invokeCompaction returns whether it found and did any compaction work
// suggested to be continuously called as long as it returns true
// since it does some part of the work each time
func (t *TableManager) invokeCompaction(workerIdx int) bool {
	if t.compactionWorkerNum == 0 {
		return false
	}

	// find the shard that has most files
	type shardInfoEntry struct {
		shardId  uint64
		numFiles int
	}
	shardInfo := make([]shardInfoEntry, t.manifest.numShards)

	t.lock.RLock()
	for i := uint64(0); i < t.manifest.numShards; i++ {
		// to avoid race condition, each worker only checks the deterministic subset of shards
		if int(i)%t.compactionWorkerNum == workerIdx {
			shardInfo[i].shardId = i
			shardInfo[i].numFiles = len(t.tables[i])
		}
	}
	t.lock.RUnlock()

	sort.Slice(shardInfo, func(i int, j int) bool {
		return shardInfo[i].numFiles < shardInfo[j].numFiles
	})

	pickedShard := shardInfo[len(shardInfo)-1] // the shard with most tables

	if pickedShard.numFiles < minimumFilesToTriggerCompaction {
		zap.L().Info("no compaction work to do for worker", zap.Int("worker_id", workerIdx))
		return false
	}

	// decide which tables in this shard to compact
	compactToFinal := false
	t.lock.RLock()
	tablesToCompact := PickTablesToCompact(t.tables[pickedShard.shardId])
	if tablesToCompact != nil && tablesToCompact[0] == t.tables[pickedShard.shardId][0] {
		compactToFinal = true
	}
	t.lock.RUnlock()
	if tablesToCompact == nil {
		zap.L().Info("no compaction work to do for worker", zap.Int("worker_id", workerIdx))
		return false
	}

	// actual compact work
	zap.L().Info("Going to compact for shard", zap.Int("worker_id", workerIdx), zap.Uint64("shardId", pickedShard.shardId), zap.Bool("compact_to_final", compactToFinal))
	newTableFile, err := CompactTables(t.manifest.dirname, tablesToCompact, pickedShard.shardId, t.manifest.tableType, compactToFinal)
	if err != nil {
		zap.L().Error("failed to compact", zap.Int("worker_id", workerIdx), zap.Error(err))
	}

	// update manifest
	t.lock.Lock()
	defer t.lock.Unlock()
	tableFilesToCompact := make([]string, 0, len(tablesToCompact))
	for _, table := range tablesToCompact {
		tableFilesToCompact = append(tableFilesToCompact, table.Name())
	}
	err = t.manifest.Replace(pickedShard.shardId, tableFilesToCompact, newTableFile)
	if err != nil {
		zap.L().Error("failed refresh manifest after compaction", zap.Int("worker_id", workerIdx), zap.Error(err))
	}

	// now sync the manifest with the current open tables by opening new ones and closing removed ones
	err = t.reloadShardTablesFromManifest(pickedShard.shardId)
	if err != nil {
		zap.L().Error("failed to reload tables after compaction", zap.Int("worker_id", workerIdx), zap.Error(err))
	}
	return true
}

func (t *TableManager) StartCompactionWorkers() {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.compactionRunning {
		return
	}

	f := func(workerIdx int) {
		defer t.wg.Done()
		zap.L().Info("compaction worker started", zap.Int("worker_id", workerIdx))
		timer := time.NewTimer(0)
		for {
			select {
			case <-timer.C:
				didWork := t.invokeCompaction(workerIdx)
				if didWork {
					timer.Reset(0)
				} else {
					// no work to do, wait for a while to do the next check
					timer.Reset(compactionPollInterval)
				}
			case _, ok := <-t.stopCh:
				if !ok {
					zap.L().Info("compaction worker stopped", zap.Int("worker_id", workerIdx))
					return
				}
			}
		}
	}

	t.stopCh = make(chan struct{})
	for i := 0; i < t.compactionWorkerNum; i++ {
		t.wg.Add(1)
		go f(i)
	}
	t.compactionRunning = true
}

func (t *TableManager) StopCompactionWorkers() {
	t.lock.Lock()
	defer t.lock.Unlock()

	if !t.compactionRunning {
		return
	}

	close(t.stopCh)
	t.wg.Wait()
	t.compactionRunning = false
}

func (t *TableManager) NumShards() uint64 {
	return t.manifest.numShards
}

func (t *TableManager) Reserve() {
	t.lock.RLock()
}

func (t *TableManager) Release() {
	t.lock.RUnlock()
}

func (t *TableManager) GetStats() []uint64 {
	ret := make([]uint64, StatsMax)
	t.lock.RLock()
	defer t.lock.RUnlock()
	for i := uint64(0); i < t.manifest.numShards; i++ {
		ret[StatsNumTables] += uint64(len(t.tables[i]))
		for _, table := range t.tables[i] {
			ret[StatsTotalReads] += table.DataReads()
			ret[StatsTotalSize] += table.Size()
			ret[StatsTotalRecords] += table.NumRecords()
			ret[StatsTotalIndexSize] += table.IndexSize()
		}
	}
	return ret
}

func (t *TableManager) List(shard uint64) ([]Table, error) {
	if shard >= t.manifest.numShards {
		return nil, fmt.Errorf("invalid shard ID")
	}
	return t.tables[uint(shard)], nil
}

func (t *TableManager) Close() error {
	t.StopCompactionWorkers()
	t.lock.Lock()
	defer t.lock.Unlock()
	for i := uint64(0); i < t.manifest.numShards; i += 1 {
		for _, t := range t.tables[i] {
			if err := t.Close(); err != nil {
				return err
			}
		}
		t.tablesByFileName[i] = nil
		t.tables[i] = nil
	}
	return nil
}

func (t *TableManager) Append(tmpTableFiles []string) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	err := t.manifest.Append(tmpTableFiles)
	if err != nil {
		return err
	}

	for i := uint64(0); i < t.manifest.numShards; i++ {
		_ = t.reloadShardTablesFromManifest(i)
	}
	return nil
}
