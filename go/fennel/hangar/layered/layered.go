package layered

import (
	"context"
	"fmt"
	"io"
	"time"

	"fennel/hangar"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/samber/mo"
	"go.uber.org/zap"
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

type fillRequest struct {
	// keygroups to fill or delete
	kgs []hangar.KeyGroup
	// flag to indicate if the request is to delete the keygroup from the cache.
	delete bool
}

type layered struct {
	planeID     ftypes.RealmID
	cache       hangar.Hangar
	db          hangar.Hangar
	fillReqChan chan fillRequest

	doneCh chan struct{}
}

var (
	cacheHits = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hangar_layered_cache_hits",
			Help: "Number of cache hits in layered store",
		},
	)
	cacheMisses = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "hangar_layered_cache_misses",
			Help: "Number of cache misses in layered store",
		},
	)
)

func (l *layered) Restore(source io.Reader) error {
	panic("implement me")
}

func (l *layered) stopFill() {
	close(l.fillReqChan)
	<-l.doneCh
}

func (l *layered) Teardown() error {
	l.stopFill()
	if err := l.cache.Teardown(); err != nil {
		return fmt.Errorf("could not tear down cache of store: %v", err)
	}
	return l.db.Teardown()
}

func (l *layered) Backup(sink io.Writer, since uint64) (uint64, error) {
	return l.db.Backup(sink, since)
}

func (l *layered) Close() error {
	l.stopFill()
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
		fillReqChan: make(chan fillRequest, 10*FILL_BATCH_SIZE),
		doneCh:      make(chan struct{}),
	}
	// TODO: if needed, shard the filling process
	go ret.processFillReqs()
	return ret
}

func (l *layered) DelMany(ctx context.Context, kgs []hangar.KeyGroup) error {
	ctx, t := timer.Start(ctx, l.planeID, "hangar.layered.delmany")
	defer t.Stop()
	if err := l.db.DelMany(ctx, kgs); err != nil {
		return fmt.Errorf("failed to delete keys from the db: %w", err)
	}
	// Initate a background "fill" request that will ensure the keygroup is
	// deleted from the cache. This is required because it is possible that the
	// processing of a fill request initiated for the same keygroup(s) is
	// executing concurrently, and has read the previous value of the keygroup
	// from the db but writes it to the cache after the DelMany finishes. Such
	// a scenario would leave the cache in a permanently inconsistent state.
	// To safeguard again this, we initiate a fill request that will delete the
	// keygroup from the cache. This ensures that the cache is always in an
	// eventually consistent state.
	l.fill(kgs, true /* delete */)
	return nil
}

func (l *layered) PlaneID() ftypes.RealmID {
	return l.planeID
}

func (l *layered) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	ctx, t := timer.Start(ctx, l.planeID, "hangar.layered.getmany")
	defer t.Stop()
	// If this read is on the write path, skip the cache since it can contain
	// stale data.
	if hangar.IsWrite(ctx) {
		vgs, err := l.db.GetMany(ctx, kgs)
		if err != nil {
			return nil, fmt.Errorf("failed to get values from the db: %w", err)
		}
		// Fill the missing keygroups in the cache.
		l.fill(kgs, false /* delete */)
		return vgs, nil
	}
	results, err := l.cache.GetMany(ctx, kgs)
	if err != nil {
		return nil, err
	}
	notfound := make([]hangar.KeyGroup, 0, len(kgs))
	ptr := arena.Ints.Alloc(0, len(kgs))
	defer arena.Ints.Free(ptr)
	for i, cval := range results {
		if kgs[i].Fields.IsAbsent() {
			var kg hangar.KeyGroup
			kg.Prefix = kgs[i].Prefix
			kg.Fields = mo.None[hangar.Fields]()
			ptr = append(ptr, i)
			notfound = append(notfound, kg)
		} else if len(cval.Fields) != len(kgs[i].Fields.MustGet()) {
			numFound := len(cval.Fields)
			cacheHits.Add(float64(numFound))
			found := make(map[string]struct{}, numFound)
			for _, field := range cval.Fields {
				found[string(field)] = struct{}{}
			}
			numRequested := len(kgs[i].Fields.MustGet())
			fields := make(hangar.Fields, 0, numRequested-numFound)
			for _, field := range kgs[i].Fields.OrEmpty() {
				if _, ok := found[string(field)]; !ok {
					fields = append(fields, field)
				}
			}
			ptr = append(ptr, i)
			notfound = append(notfound, hangar.KeyGroup{
				Prefix: kgs[i].Prefix,
				Fields: mo.Some(fields),
			})
			cacheMisses.Add(float64(len(fields)))
		} else {
			cacheHits.Add(float64(len(cval.Fields)))
		}
	}

	if len(notfound) == 0 {
		return results, nil
	}
	dbvals, err := l.db.GetMany(ctx, notfound)
	if err != nil {
		return nil, fmt.Errorf("failed to get values from the db: %w", err)
	}
	for i, dbval := range dbvals {
		if len(dbval.Fields) > 0 {
			if err = results[ptr[i]].Update(dbval); err != nil {
				return nil, fmt.Errorf("failed to update valgroup: %w", err)
			}
		}
	}
	// Fill the missing keygroups in the cache.
	l.fill(notfound, false /* delete */)
	return results, nil
}

func (l *layered) SetMany(ctx context.Context, keys []hangar.Key, vgs []hangar.ValGroup) error {
	ctx, t := timer.Start(ctx, l.planeID, "hangar.layered.setmany")
	defer t.Stop()
	kgs := make([]hangar.KeyGroup, len(keys))
	for i, key := range keys {
		kgs[i].Prefix = key
		kgs[i].Fields = mo.Some(vgs[i].Fields)
	}
	if err := l.db.SetMany(ctx, keys, vgs); err != nil {
		return err
	}
	l.fill(kgs, false /* delete */)
	return nil
}

func (l *layered) fill(kgs []hangar.KeyGroup, delete bool) {
	for i := 0; i < len(kgs); i += FILL_BATCH_SIZE {
		end := i + FILL_BATCH_SIZE
		if end > len(kgs) {
			end = len(kgs)
		}
		l.fillReqChan <- fillRequest{kgs[i:end], delete}
	}
}

func (l *layered) processFillReqs() {
	defer close(l.doneCh)
	// Allocate two separate arrays for keygroups to update and delete.
	updates := [2 * FILL_BATCH_SIZE]hangar.KeyGroup{}
	deletions := [2 * FILL_BATCH_SIZE]hangar.KeyGroup{}
	timeout := FILL_TIMEOUT_MS * time.Millisecond
	for {
		updateBatch := updates[:0]
		deletionBatch := deletions[:0]
		timer := time.NewTimer(timeout)
	FILL:
		for len(updateBatch) < FILL_BATCH_SIZE && len(deletionBatch) < FILL_BATCH_SIZE {
			select {
			case req, ok := <-l.fillReqChan:
				if !ok {
					return
				}
				if req.delete {
					deletionBatch = append(deletionBatch, req.kgs...)
				} else {
					updateBatch = append(updateBatch, req.kgs...)
				}
			case <-timer.C:
				break FILL
			}
		}
		// Stop the timer explicitly to make it eligible for garbage collection.
		_ = timer.Stop()
		if len(deletionBatch) > 0 {
			if err := l.cache.DelMany(context.Background(), deletionBatch); err != nil {
				zap.L().Warn("Failed to delete from cache", zap.Error(err))
				continue
			}
		}
		if len(updateBatch) > 0 {
			dbvals, err := l.db.GetMany(context.Background(), updateBatch)
			if err != nil {
				zap.L().Warn("Failed to get values from db", zap.Error(err))
				continue
			}
			keys := make([]hangar.Key, 0, len(updateBatch))
			valgroups := make([]hangar.ValGroup, 0, len(updateBatch))
			for i, dbval := range dbvals {
				// Initialize all fields as empty. This allows us to remember in
				// the cache that some keys and/or fields are missing from the db.
				fields := updateBatch[i].Fields.OrEmpty()
				vg := hangar.ValGroup{
					Fields: fields,
					Values: make(hangar.Values, len(fields)),
				}
				err = vg.Update(dbval)
				if err != nil {
					zap.L().Warn("Failed to update valgroup", zap.Error(err))
				}
				keys = append(keys, updateBatch[i].Prefix)
				valgroups = append(valgroups, vg)
			}
			if err = l.cache.SetMany(context.Background(), keys, valgroups); err != nil {
				zap.L().Warn("Failed to fill cache", zap.Error(err))
				continue
			}
		}
	}
}

var _ hangar.Hangar = (*layered)(nil)
