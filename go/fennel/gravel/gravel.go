package gravel

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

/*
	Gravel is a fast key/Value store that is a hybrid of LSM and Btrees. It makes one
	key assumption -- it's okay to lose the last few writes as long as each commit
	batch is either persisted atomically on the disk or it is not. In other words,
	disk persistence represents batch boundaries. This relaxation of correctness
	enables Gravel to support a VERY high burst commit throughput.

	Gravel is represented by a series of files on the disk with *.grvl extension. Each
	file's name is an uint64 number. Files written later (which contain more recent
	data) have a higher index number. This index number of each file helps Gravel
	understand which files to look into first ahead of others.

*/

type Gravel struct {
	memtable   Memtable
	manifest   *Manifest
	commitlock sync.Mutex
	opts       Options
	stats      Stats
	// TODO(mohit): Consider adding back periodic flushing if the memtable has not reached it's size limit for a while.
	// This can happen when the write throughput is not high - and we might want to write the tables periodically
	// to avoid startup and binlog catchup latency.
}

func Open(opts Options) (ret *Gravel, failure error) {
	if opts.TableType == testTable {
		// testTable is only for testing, not for prod use cases
		return nil, fmt.Errorf("invalid table type: %d", testTable)
	}
	manifest, err := InitManifest(opts.Dirname, opts.TableType, opts.NumShards)
	if err != nil {
		return nil, fmt.Errorf("could not init manifest: %w", err)
	}
	// if the DB was earlier created with a different number of shards
	// manifest would have picked that one instead
	opts.NumShards = manifest.numShards
	ret = &Gravel{
		memtable:   NewMemTable(manifest.numShards),
		manifest:   manifest,
		opts:       opts,
		commitlock: sync.Mutex{},
		stats:      Stats{},
	}
	go ret.reportStats()
	return ret, nil
}

func (g *Gravel) Get(key []byte) ([]byte, error) {
	hash := Hash(key)
	sample := shouldSample()
	maybeInc(sample, &g.stats.Gets)
	now := Timestamp(time.Now().Unix())
	val, err := g.memtable.Get(key, hash)
	switch err {
	case ErrNotFound:
		// do nothing, we will just check it in all the tables
		maybeInc(sample, &g.stats.MemtableMisses)
	case nil:
		maybeInc(sample, &g.stats.MemtableHits)
		return handle(val, now)
	default:
		return nil, err
	}
	shard := hash & (g.manifest.numShards - 1)
	g.manifest.Lock()
	defer g.manifest.Unlock()
	tables, err := g.manifest.List(shard)
	if err != nil {
		return nil, fmt.Errorf("invalid shard: %w", err)
	}
	for _, table := range tables {
		maybeInc(sample, &g.stats.TableIndexReads)
		val, err := table.Get(key, hash)
		switch err {
		case ErrNotFound:
		case nil:
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
	if batchsz > g.opts.MaxTableSize {
		// this batch is so large that it won't fit in any single memtable
		return errors.New("commit batch too large")
	}
	if g.memtable.Size()+batchsz > g.opts.MaxTableSize {
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

func (g *Gravel) Teardown() error {
	if err := g.Close(); err != nil {
		return err
	}
	return os.RemoveAll(g.opts.Dirname)
}

func (g *Gravel) Close() error {
	return g.manifest.Close()
}

// NOTE: the caller of flush is expected to hold commitlock
func (g *Gravel) flush() error {
	if g.memtable.Size() == 0 {
		// no valid reason to flush an empty memtable
		return nil
	}
	tablefiles, err := g.memtable.Flush(g.opts.TableType, g.opts.Dirname, g.manifest.numShards)
	if err != nil {
		return err
	}
	maybeInc(true, &g.stats.NumTableBuilds)
	if err = g.manifest.Append(tablefiles); err != nil {
		return err
	}
	if err = g.memtable.Clear(); err != nil {
		return err
	}
	g.stats.MemtableSizeBytes.Store(0)
	g.stats.MemtableKeys.Store(0)
	return err
}
