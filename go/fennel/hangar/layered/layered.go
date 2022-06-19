package layered

import (
	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/test"
	"fmt"
	"io"
	"time"

	"github.com/samber/mo"
)

/*
	 Layered is a Store that is composed of two layers of stores.
	 The first layer acts as cache and the second layer acts as the ground truth.

     During read time, the first layer is checked first. If the key is not found,
	 the second layer is checked. If the key is found, the value is returned and
	 the key is added to the cache. During write time, the key is added to both
	 layers.

	 This struct is responsible for maintaining cache consistency. It is eventually
	 consistent and does so by the following mechanisms:
	 1. All cache sets are done by a single writer to the cache (and that path is
	    called "fill"). Note: deletes can be done by multiple writers.
		cache keys can be deleted from multiple places but sets
	 2. When we write to layered store, we first invalidate the cache, write to the
		ground truth, and then enqueue the key to be backfilled into the cache.
     3. During fill time, we always read from the ground truth before writing to
		the cache.


	NOTES:
	1. If the underlying cache/db stores are thread safe, so is this store -- Gets are
       natively thread safe, and sets are serialized in the fill path.
	2. Because of the fill path, there is upto 100ms delay in filling the cache. In that
	   duration, we might see cache misses.
	3. The set throughput is limited by the fill path throughput. Currently, fill path is
	   not sharded, but it can be done so trivially if needed someday (we just need to ensure
	   that all updates to a single cache key always go to the same shard).

*/

const (
	FILL_BATCH_SIZE = 1000
	FILL_TIMEOUT_MS = 10
)

type layered struct {
	planeID     ftypes.RealmID
	cache       hangar.Hangar
	db          hangar.Hangar
	fillReqChan chan []hangar.KeyGroup
}

func (l *layered) Restore(source io.Reader) error {
	panic("implement me")
}

// TODO: close all goroutines as part of teardown
func (l *layered) Teardown() error {
	if !test.IsInTest() {
		return fmt.Errorf("can not teardown a store outside of test mode")
	}
	if err := l.cache.Teardown(); err != nil {
		return fmt.Errorf("could not tear down cache of store: %v", err)
	}
	return l.db.Teardown()
}

func (l *layered) Backup(sink io.Writer, since uint64) (uint64, error) {
	return l.db.Backup(sink, since)
}

func (l *layered) Close() error {
	if err := l.cache.Close(); err != nil {
		return err
	}
	return l.db.Close()
}

func NewHangar(planeID ftypes.RealmID, cache, db hangar.Hangar) hangar.Hangar {
	ret := &layered{
		planeID:     planeID,
		cache:       cache,
		db:          db,
		fillReqChan: make(chan []hangar.KeyGroup, 10*FILL_BATCH_SIZE),
	}
	// TODO: if needed, shard the filling process
	go ret.processFillReqs()
	return ret
}

func (l *layered) DelMany(keys []hangar.KeyGroup) error {
	err := l.cache.DelMany(keys)
	if err != nil {
		return err
	}
	if err = l.db.DelMany(keys); err != nil {
		return err
	}
	l.fill(keys)
	return nil
}

func (l *layered) PlaneID() ftypes.RealmID {
	return l.planeID
}

func (l *layered) Encoder() hangar.Encoder {
	return l.cache.Encoder()
}

func (l *layered) GetMany(kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	results, err := l.cache.GetMany(kgs)
	if err != nil {
		return nil, err
	}
	notfound := make([]hangar.KeyGroup, 0, len(kgs))
	ptr := make([]int, len(kgs))

	for i, cval := range results {
		if kgs[i].Fields.IsAbsent() {
			var kg hangar.KeyGroup
			kg.Prefix = kgs[i].Prefix
			kg.Fields = mo.None[hangar.Fields]()
			ptr[i] = len(notfound)
			notfound = append(notfound, kg)
		} else if len(cval.Fields) != len(kgs[i].Fields.MustGet()) {
			found := make(map[string]struct{}, len(kgs[i].Fields.MustGet()))
			for _, field := range cval.Fields {
				found[string(field)] = struct{}{}
			}
			var kg hangar.KeyGroup
			fields := make(hangar.Fields, 0, len(kgs[i].Fields.MustGet())-len(cval.Fields))
			for _, field := range kgs[i].Fields.OrEmpty() {
				if _, ok := found[string(field)]; !ok {
					fields = append(fields, field)
				}
			}
			kg.Prefix = kgs[i].Prefix
			kg.Fields = mo.Some(fields)
			ptr[i] = len(notfound)
			notfound = append(notfound, kg)
		} else {
			ptr[i] = -1
		}
	}

	if len(notfound) == 0 {
		return results, nil
	}
	dbvals, err := l.db.GetMany(notfound)
	if err != nil {
		return results, err
	}
	tofill := make([]hangar.KeyGroup, 0, len(notfound))
	for i, dbval := range dbvals {
		if len(dbval.Fields) > 0 {
			results[ptr[i]].Update(dbval)
			tofill = append(tofill, notfound[i])
		}
	}

	// fill whatever cache misses we saw
	if len(tofill) > 0 {
		l.fill(tofill)
	}
	return results, nil
}

func (l *layered) SetMany(keys []hangar.Key, vgs []hangar.ValGroup) error {
	kgs := make([]hangar.KeyGroup, len(keys))
	for i, key := range keys {
		kgs[i].Prefix = key
		kgs[i].Fields = mo.Some(vgs[i].Fields)
	}
	if err := l.cache.DelMany(kgs); err != nil {
		return err
	}
	if err := l.db.SetMany(keys, vgs); err != nil {
		return err
	}
	return l.fill(kgs)
}

func (l *layered) fill(kgs []hangar.KeyGroup) error {
	for i := 0; i < len(kgs); i += FILL_BATCH_SIZE {
		end := i + FILL_BATCH_SIZE
		if end > len(kgs) {
			end = len(kgs)
		}
		l.fillReqChan <- kgs[i:end]
	}
	return nil
}

func (l *layered) processFillReqs() {
	arr := [2 * FILL_BATCH_SIZE]hangar.KeyGroup{}
	for {
		batch := arr[:0]
		batch = append(batch, <-l.fillReqChan...)
		tick := time.After(FILL_TIMEOUT_MS * time.Millisecond)
	POLL:
		for len(batch) < FILL_BATCH_SIZE {
			select {
			case kgs := <-l.fillReqChan:
				batch = append(batch, kgs...)
			case <-tick:
				break POLL
			}
		}
		dbvals, err := l.db.GetMany(batch)
		if err != nil {
			continue
		}
		keys := make([]hangar.Key, 0, len(batch))
		valgroups := make([]hangar.ValGroup, 0, len(batch))

		for i, dbval := range dbvals {
			if len(dbval.Fields) == 0 {
				// nothing to put in cache
				continue
			}
			keys = append(keys, batch[i].Prefix)
			valgroups = append(valgroups, dbval)
		}
		l.cache.SetMany(keys, valgroups)
	}
}

var _ hangar.Hangar = (*layered)(nil)
