package gravel

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/raulk/clock"
	"go.uber.org/zap"
)

/*
	Gravel is a fast key/Value store that is a hybrid of LSM and Hash Tables.

	It is a general purpose disk backed k-v store with just two relaxations on
	semantics compared to a typical LSM database --
		1. It assumes that it's okay to lose the last few writes as long as each
		commit batch is either persisted atomically on the disk or it is not. In
		other words, disk persistence respects batch boundaries.

		2. Unlike conventional LSMs, it doesn't support sorted forward/backward
		iteration on key space.

*/

const periodicFlushTickerDur = 10 * time.Minute

var tablesQueriedReporter = promauto.NewSummaryVec(prometheus.SummaryOpts{
	Name: "number_of_tables_queried_for_gravel_value",
	Help: "The number of tables queried when there is a query hit",
	Objectives: map[float64]float64{
		0.25: 0.05,
		0.50: 0.05,
		0.75: 0.05,
		0.90: 0.05,
		0.95: 0.02,
		0.99: 0.01,
	},
}, []string{"realm_id"})

type Gravel struct {
	memtables    [2]*Memtable
	memtableLock sync.RWMutex
	flushingChan chan struct{}
	flushingWg   sync.WaitGroup

	tm                  *TableManager
	opts                Options
	stats               Stats
	closeCh             chan struct{}
	periodicFlushTicker *time.Ticker
	logger              *zap.Logger
	clock               clock.Clock
}

func Open(opts Options, clock clock.Clock) (ret *Gravel, failure error) {
	if opts.TableType == testTable {
		// testTable is only for testing, not for prod use cases
		return nil, fmt.Errorf("invalid table type: %d", testTable)
	}
	logger := zap.L().Named(opts.Name)
	tableManager, err := InitTableManager(opts.Dirname, opts.TableType, opts.NumShards, opts.CompactionWorkerNum, logger)
	if err != nil {
		return nil, fmt.Errorf("could not init manifest: %w", err)
	}
	// if the DB was earlier created with a different number of shards, use the existing value in the DB
	opts.NumShards = tableManager.NumShards()
	ret = &Gravel{
		memtableLock:        sync.RWMutex{},
		flushingChan:        make(chan struct{}, 1),
		flushingWg:          sync.WaitGroup{},
		tm:                  tableManager,
		opts:                opts,
		stats:               Stats{},
		closeCh:             make(chan struct{}, 1),
		periodicFlushTicker: time.NewTicker(periodicFlushTickerDur),
		logger:              logger,
		clock:               clock,
	}
	ret.memtables[0] = NewMemTable(tableManager.NumShards())
	ret.memtables[1] = NewMemTable(tableManager.NumShards())
	go ret.periodicallyFlush()
	go ret.reportStats()
	return ret, nil
}

func (g *Gravel) Get(key []byte) ([]byte, error) {
	tablesQueried := 0
	hash := Hash(key)
	shard := Shard(hash, g.tm.NumShards())
	sample := shouldSample()

	maybeInc(sample, &g.stats.Gets)
	now := Timestamp(g.clock.Now().Unix())

	g.memtableLock.RLock()
	for _, mt := range g.memtables {
		val, err := mt.Get(key, shard)
		switch err {
		case ErrNotFound:
		case nil:
			g.memtableLock.RUnlock()
			maybeInc(sample, &g.stats.MemtableHits)
			if shouldSampleEvery1024() {
				tablesQueriedReporter.WithLabelValues("0").Observe(float64(tablesQueried))
			}
			return handle(val, now)
		default:
			g.memtableLock.RUnlock()
			return nil, err
		}
	}
	g.memtableLock.RUnlock()
	maybeInc(sample, &g.stats.MemtableMisses)

	g.tm.Reserve()
	defer g.tm.Release()
	tables, err := g.tm.List(shard)
	if err != nil {
		return nil, fmt.Errorf("invalid shard: %w", err)
	}
	for i := len(tables) - 1; i >= 0; i-- {
		table := tables[i]
		maybeInc(sample, &g.stats.TableIndexReads)
		val, err := table.Get(key, hash)
		tablesQueried++
		switch err {
		case ErrNotFound:
		case nil:
			if shouldSampleEvery1024() {
				tablesQueriedReporter.WithLabelValues("0").Observe(float64(tablesQueried))
			}
			return handle(val, now)
		default:
			return nil, err
		}
	}
	maybeInc(sample, &g.stats.Misses)
	return nil, ErrNotFound
}

func (g *Gravel) NewBatch() *Batch {
	return &Batch{
		gravel: g,
	}
}

func (g *Gravel) commit(batch *Batch) error {
	batchsz := uint64(0)
	for _, e := range batch.Entries() {
		batchsz += uint64(sizeof(e))
	}
	if batchsz > g.opts.MaxMemtableSize {
		// this batch is so large that it won't fit in any single memtable
		return errors.New("commit batch too large")
	}
	if g.memtables[0].Size()+batchsz > g.opts.MaxMemtableSize {
		// flush so that this commit can go to the next memtable
		if err := g.flush(); err != nil {
			return err
		}
	}
	g.memtableLock.RLock()
	defer g.memtableLock.RUnlock()
	// batch can fit in a single memtable, so set it now
	return g.memtables[0].SetMany(batch.Entries(), &g.stats)
}

func handle(val Value, now Timestamp) ([]byte, error) {
	if val.deleted || isExpired(val.expires, now) {
		return nil, ErrNotFound
	} else {
		return val.data, nil
	}
}

func isExpired(expires, now Timestamp) bool {
	return expires > 0 && expires < now
}

func (g *Gravel) StartCompaction() error {
	g.tm.StartCompactionWorkers()
	return nil
}

func (g *Gravel) StopCompaction() error {
	g.tm.StopCompactionWorkers()
	return nil
}

func (g *Gravel) Teardown() error {
	if err := g.Close(); err != nil {
		return err
	}
	return os.RemoveAll(g.opts.Dirname)
}

func (g *Gravel) Close() error {
	if err := g.Flush(); err != nil {
		return err
	}
	g.flushingWg.Wait()
	// notify that the db has been closed
	g.closeCh <- struct{}{}
	return g.tm.Close()
}

func (g *Gravel) Backup() error {
	return g.tm.Close()
}

func (g *Gravel) Flush() error {
	return g.flush()
}

// TODO(mohit): Expose Flush as a public method for testing!

func (g *Gravel) flush() error {
	defer g.periodicFlushTicker.Reset(periodicFlushTickerDur)
	g.memtableLock.Lock()
	if g.memtables[0].Size() == 0 {
		// no valid reason to flush an empty memtable
		g.memtableLock.Unlock()
		return nil
	}
	g.memtableLock.Unlock()

	g.flushingChan <- struct{}{}
	g.memtableLock.Lock()
	if g.memtables[1].Size() != 0 {
		panic("failed flush of memtable")
	}

	if g.memtables[0].Size() == 0 {
		// no valid reason to flush an empty memtable
		g.memtableLock.Unlock()
		<-g.flushingChan
		return nil
	}
	// now the shadow memtable (1) must be emptied
	g.memtables[0], g.memtables[1] = g.memtables[1], g.memtables[0]
	g.memtableLock.Unlock()

	g.flushingWg.Add(1)
	go func() {
		defer g.flushingWg.Done()
		// since a flush is being attempted, reset the periodic flush
		tablefiles, err := g.memtables[1].Flush(g.opts.TableType, g.opts.Dirname)

		// we probably should panic(), but now it's just blocking the next flush
		// at least we shouldn't let new data come in
		if err != nil {
			g.logger.Error("failed to flush", zap.Error(err))
			return
		}
		if err = g.tm.Append(tablefiles); err != nil {
			g.logger.Error("failed to flush", zap.Error(err))
			return
		}
		if err = g.memtables[1].Clear(); err != nil {
			g.logger.Error("failed to flush", zap.Error(err))
			return
		}

		maybeInc(true, &g.stats.NumTableBuilds)
		g.stats.MemtableSizeBytes.Store(g.memtables[0].Size())
		g.stats.MemtableKeys.Store(g.memtables[0].Len())

		<-g.flushingChan
	}()
	return nil
}

// If write volume is low, memtable may not reach tablesize for
// a while, and so may not flush. While it's technically not an issue,
// flushing doesn't hurt us and can make future startup faster.
// This function forces a flush 10 minutes after the last natural flush.
func (g *Gravel) periodicallyFlush() {
	for {
		select {
		case <-g.periodicFlushTicker.C:
			func() {
				if err := g.flush(); err != nil {
					g.logger.Warn("periodic flush failed", zap.Error(err))
				}
			}()
		case <-g.closeCh:
			g.periodicFlushTicker.Stop()
			return
		}
	}
}
