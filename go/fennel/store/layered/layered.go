package layered

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/test"
	"fmt"
	"io"
	"time"
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
	cache       store.Store
	db          store.Store
	fillReqChan chan []store.KeyGroup
}

func (l *layered) Restore(source io.Reader) error {
	panic("implement me")
}

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

func NewStore(planeID ftypes.RealmID, cache, db store.Store) store.Store {
	ret := &layered{
		planeID:     planeID,
		cache:       cache,
		db:          db,
		fillReqChan: make(chan []store.KeyGroup, 10*FILL_BATCH_SIZE),
	}
	// TODO: if needed, shard the filling process
	go ret.processFillReqs()
	return ret
}

func (l *layered) DelMany(keys []store.KeyGroup) error {
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

func (l *layered) Encoder() store.Encoder {
	return l.cache.Encoder()
}

func (l *layered) GetMany(kgs []store.KeyGroup) ([]store.ValGroup, error) {
	results, err := l.cache.GetMany(kgs)
	if err != nil {
		return nil, err
	}
	notfound := make([]store.KeyGroup, 0, len(kgs))
	ptr := make([]int, len(kgs))

	for i, cval := range results {
		if len(cval.Fields) != len(kgs[i].Fields) {
			found := make(map[string]struct{}, len(kgs[i].Fields))
			for _, field := range kgs[i].Fields {
				found[string(field)] = struct{}{}
			}
			var kg store.KeyGroup
			for _, field := range kgs[i].Fields {
				if _, ok := found[string(field)]; !ok {
					kg.Fields = append(kg.Fields, field)
				}
			}
			kg.Prefix = kgs[i].Prefix
			ptr[i] = len(notfound)
			notfound = append(notfound, kg)
		} else {
			ptr[i] = -1
		}
	}

	if len(notfound) == 0 {
		return results, nil
	}
	tofill := make([]store.KeyGroup, 0, len(notfound))
	dbvals, err := l.db.GetMany(notfound)
	if err != nil {
		return results, err
	}
	for i, dbval := range dbvals {
		results[i].Update(dbval)
		if len(results[i].Fields) > 0 {
			tofill = append(tofill, notfound[i])
		}
	}

	// fill whatever cache misses we saw
	if len(tofill) > 0 {
		l.fill(tofill)
	}
	return results, nil
}

func (l *layered) SetMany(keys []store.Key, vgs []store.ValGroup) error {
	kgs := make([]store.KeyGroup, len(keys))
	for i, key := range keys {
		kgs[i].Prefix = key
		kgs[i].Fields = vgs[i].Fields
	}
	if err := l.cache.DelMany(kgs); err != nil {
		return err
	}
	if err := l.db.SetMany(keys, vgs); err != nil {
		return err
	}
	return l.fill(kgs)
}

func (l *layered) fill(kgs []store.KeyGroup) error {
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
	arr := [2 * FILL_BATCH_SIZE]store.KeyGroup{}
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
		keys := make([]store.Key, 0, len(batch))
		valgroups := make([]store.ValGroup, 0, len(batch))

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

var _ store.Store = (*layered)(nil)
