package gravel

import (
	"errors"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"strings"
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
	memtable      Memtable
	tableList     []Table
	tableListLock sync.RWMutex
	commitlock    sync.Mutex
	opts          Options
	stats         Stats
	// TODO(mohit): Consider adding back periodic flushing if the memtable has not reached it's size limit for a while.
	// This can happen when the write throughput is not high - and we might want to write the tables periodically
	// to avoid startup and binlog catchup latency.
}

func Open(opts Options) (ret *Gravel, failure error) {
	// if the directory doesn't exist, create it
	if err := os.MkdirAll(opts.Dirname, os.ModePerm); err != nil {
		return nil, err
	}
	ret = &Gravel{
		memtable:      NewMemTable(),
		tableListLock: sync.RWMutex{},
		opts:          opts,
		commitlock:    sync.Mutex{},
		stats:         Stats{},
	}
	files, err := ioutil.ReadDir(opts.Dirname)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		tname := file.Name()
		if !strings.HasSuffix(tname, SUFFIX) {
			continue
		}
		table, err := OpenTable(opts.TableType, path.Join(opts.Dirname, tname))
		if err != nil {
			return nil, err
		}
		ret.addTable(table)
	}
	go ret.reportStats()
	return ret, nil
}

func (g *Gravel) Get(key []byte) ([]byte, error) {
	sample := shouldSample()
	maybeInc(sample, &g.stats.Gets)
	now := Timestamp(time.Now().Unix())
	val, err := g.memtable.Get(key)
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
	g.tableListLock.RLock()
	defer g.tableListLock.RUnlock()
	for _, table := range g.tableList {
		maybeInc(sample, &g.stats.TableIndexReads)
		val, err := table.Get(key)
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

// nextID returns an ID number for the next table file.
// We want successive indices to be sufficiently far apart so that when we
// do compactions, we can find numbers between any two existing indices.
func (g *Gravel) nextID() uint64 {
	maxsofar := uint64(0)
	for _, t := range g.tableList {
		id := t.ID()
		if id > maxsofar {
			maxsofar = id
		}
	}
	return maxsofar + 100_000
}

func (g *Gravel) addTable(t Table) {
	g.tableListLock.Lock()
	defer g.tableListLock.Unlock()
	g.tableList = append(g.tableList, t)
	sort.Slice(g.tableList, func(i, j int) bool {
		return g.tableList[i].ID() > g.tableList[j].ID()
	})
	g.stats.NumTables.Store(uint64(len(g.tableList)))
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
	g.tableListLock.Lock()
	defer g.tableListLock.Unlock()

	for _, t := range g.tableList {
		if err := t.Close(); err != nil {
			return err
		}
	}
	return nil
}

// NOTE: the caller of flush is expected to hold commitlock
func (g *Gravel) flush() error {
	if g.memtable.Size() == 0 {
		// no valid reason to flush an empty memtable
		return nil
	}
	table, err := g.memtable.Flush(g.opts.TableType, g.opts.Dirname, g.nextID())
	if err != nil {
		return err
	}
	maybeInc(true, &g.stats.NumTableBuilds)
	g.addTable(table)
	if err = g.memtable.Clear(); err != nil {
		return err
	}
	g.stats.MemtableSizeBytes.Store(0)
	g.stats.MemtableKeys.Store(0)
	return err
}
