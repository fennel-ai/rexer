package gravel

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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
	memtable            *Memtable
	tm                  *TableManager
	commitlock          sync.Mutex
	opts                Options
	stats               Stats
	closeCh             chan struct{}
	periodicFlushTicker *time.Ticker
	logger              *zap.Logger
}

func Open(opts Options) (ret *Gravel, failure error) {
	if opts.TableType == testTable {
		// testTable is only for testing, not for prod use cases
		return nil, fmt.Errorf("invalid table type: %d", testTable)
	}
	logger := zap.L().Named(opts.Name)
	// manifest, err := InitManifest(opts.Dirname, opts.TableType, opts.NumShards)
	tableManager, err := InitTableManager(opts.Dirname, opts.TableType, opts.NumShards, opts.CompactionWorkerNum, logger)
	if err != nil {
		return nil, fmt.Errorf("could not init manifest: %w", err)
	}
	// if the DB was earlier created with a different number of shards, use the existing value in the DB
	opts.NumShards = tableManager.NumShards()
	ret = &Gravel{
		memtable:            NewMemTable(tableManager.NumShards()),
		tm:                  tableManager,
		opts:                opts,
		commitlock:          sync.Mutex{},
		stats:               Stats{},
		closeCh:             make(chan struct{}, 1),
		periodicFlushTicker: time.NewTicker(periodicFlushTickerDur),
		logger:              logger,
	}
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
	now := Timestamp(time.Now().Unix())
	val, err := g.memtable.Get(key, shard)
	switch err {
	case ErrNotFound:
		// do nothing, we will just check it in all the tables
		maybeInc(sample, &g.stats.MemtableMisses)
	case nil:
		maybeInc(sample, &g.stats.MemtableHits)
		if shouldSampleEvery1024() {
			tablesQueriedReporter.WithLabelValues("0").Observe(float64(tablesQueried))
		}
		return handle(val, now)
	default:
		return nil, err
	}
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
	g.commitlock.Lock()
	defer g.commitlock.Unlock()
	batchsz := uint64(0)
	for _, e := range batch.Entries() {
		batchsz += uint64(sizeof(e))
	}
	if batchsz > g.opts.MaxMemtableSize {
		// this batch is so large that it won't fit in any single memtable
		return errors.New("commit batch too large")
	}
	if g.memtable.Size()+batchsz > g.opts.MaxMemtableSize {
		// flush so that this commit can go to the next memtable
		if err := g.flush(); err != nil {
			return err
		}
	}
	// batch can fit in a single memtable, so set it now
	return g.memtable.SetMany(batch.Entries(), &g.stats)
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

// NOTE: the caller of flush is expected to hold commitlock
func (g *Gravel) flush() error {
	if g.memtable.Size() == 0 {
		// no valid reason to flush an empty memtable
		return nil
	}
	// since a flush is being attempted, reset the periodic flush
	g.periodicFlushTicker.Reset(periodicFlushTickerDur)
	tablefiles, err := g.memtable.Flush(g.opts.TableType, g.opts.Dirname)
	if err != nil {
		return err
	}
	maybeInc(true, &g.stats.NumTableBuilds)
	if err = g.tm.Append(tablefiles); err != nil {
		return err
	}
	if err = g.memtable.Clear(); err != nil {
		return err
	}
	g.stats.MemtableSizeBytes.Store(0)
	g.stats.MemtableKeys.Store(0)
	return err
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
				g.commitlock.Lock()
				defer g.commitlock.Unlock()
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
