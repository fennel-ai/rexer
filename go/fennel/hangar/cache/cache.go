package cache

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"fennel/hangar"
	"fennel/lib/ftypes"
	rstats "fennel/lib/ristretto"
	"fennel/lib/timer"
	"fennel/lib/utils/parallel"

	"github.com/dgraph-io/ristretto"
	"github.com/raulk/clock"
)

const (
	// CACHE_BATCH_SIZE the size of the batches in which we break down
	// incoming get calls
	CACHE_BATCH_SIZE = 1000
	PARALLELISM      = 64
)

func NewHangar(planeId ftypes.RealmID, maxSize, avgSize uint64, enc hangar.Encoder) (*rcache, error) {
	config := &ristretto.Config{
		BufferItems: 64,
		NumCounters: 10 * int64(maxSize/avgSize),
		MaxCost:     int64(maxSize),
		Metrics:     true,
	}
	cache, err := ristretto.NewCache(config)
	if err != nil {
		return nil, err
	}
	// Start reporting cache stats periodically.
	rstats.ReportPeriodically("hangar", cache, 10*time.Second)

	ret := rcache{
		planeID:    planeId,
		cache:      cache,
		enc:        enc,
		workerPool: parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_cache", PARALLELISM),
	}
	return &ret, nil
}

type rcache struct {
	enc        hangar.Encoder
	planeID    ftypes.RealmID
	cache      *ristretto.Cache
	workerPool *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
}

func (c *rcache) StartCompaction() error {
	//TODO implement me
	panic("implement me")
}

func (c *rcache) StopCompaction() error {
	//TODO implement me
	panic("implement me")
}

func (c *rcache) Flush() error {
	//TODO implement me
	panic("implement me")
}

var _ hangar.Hangar = &rcache{}

func (c *rcache) Teardown() error {
	return c.Close()
}

func (c *rcache) Backup(sink io.Writer, since uint64) (uint64, error) {
	return 0, fmt.Errorf("can not backup a cache store")
}

func (c *rcache) Encoder() hangar.Encoder {
	return c.enc
}

func (c *rcache) Close() error {
	c.workerPool.Close()
	c.cache.Close()
	return nil
}

func (c *rcache) PlaneID() ftypes.RealmID {
	return c.planeID
}

// GetMany returns the values for the given keyGroups.
// It parallelizes the requests to the underlying cache upto a degree of parallelism
func (c *rcache) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	ctx, t := timer.Start(ctx, c.planeID, "hangar.cache.getmany")
	defer t.Stop()
	// We try to spread across available workers while giving each worker
	// a minimum of CACHE_BATCH_SIZE keyGroups to work on.
	batch := len(kgs) / PARALLELISM
	if batch < CACHE_BATCH_SIZE {
		batch = CACHE_BATCH_SIZE
	}
	return c.workerPool.Process(ctx, kgs, func(kgs []hangar.KeyGroup, vgs []hangar.ValGroup) error {
		_, t := timer.Start(ctx, c.planeID, "hangar.cache.getmany.batch")
		defer t.Stop()
		eks, err := hangar.EncodeKeyManyKG(kgs, c.enc)
		if err != nil {
			return fmt.Errorf("error encoding key: %w", err)
		}
		for i, ek := range eks {
			val, found := c.cache.Get(ek)
			if !found {
				// not found, this is not an error, so we will just return empty ValGroup
				continue
			}
			if asbytes, ok := val.([]byte); !ok {
				return fmt.Errorf("cache: expected []byte, got %T", val)
			} else if _, err := c.enc.DecodeVal(asbytes, &vgs[i], true); err != nil {
				return fmt.Errorf("error decoding value: %w", err)
			} else {
				if kgs[i].Fields.IsPresent() {
					vgs[i].Select(kgs[i].Fields.MustGet())
				}
			}
		}
		return nil
	}, batch)
}

// SetMany sets many keyGroups in a single transaction. Since these are all set in a single
// transaction, there is no parallelism. If parallelism is desired, create batches of
// keyGroups and call SetMany on each batch.
func (c *rcache) SetMany(ctx context.Context, keys []hangar.Key, deltas []hangar.ValGroup) error {
	_, t := timer.Start(ctx, c.planeID, "hangar.cache.setmany")
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
	eks, err := hangar.EncodeKeyMany(keys, c.enc)
	if err != nil {
		return nil
	}
	for i, ek := range eks {
		var oldvg hangar.ValGroup
		val, found := c.cache.Get(ek)
		if found {
			asbytes, ok := val.([]byte)
			if !ok {
				return fmt.Errorf("cache value %v for key %v is not a []byte", val, keys[i])
			}
			if _, err := c.enc.DecodeVal(asbytes, &oldvg, true); err != nil {
				return err
			}
		}
		if err := oldvg.Update(deltas[i]); err != nil {
			return err
		}
		deltas[i] = oldvg
	}
	return c.commit(eks, deltas, nil)
}

func (c *rcache) DelMany(ctx context.Context, keyGroups []hangar.KeyGroup) error {
	_, t := timer.Start(ctx, c.planeID, "hangar.cache.delmany")
	defer t.Stop()
	eks, err := hangar.EncodeKeyManyKG(keyGroups, c.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]hangar.ValGroup, 0, len(keyGroups))
	delKeys := make([][]byte, 0, len(keyGroups))
	for i, ek := range eks {
		cval, found := c.cache.Get(ek)
		if !found {
			// nothing to delete
			continue
		}
		asbytes, ok := cval.([]byte)
		if !ok {
			// this should never happen
			// so log an error and clean up
			log.Printf("key %s of type %T is not a []byte ", ek, cval)
			c.cache.Del(ek)
			continue
		}
		var asvg hangar.ValGroup
		if _, err := c.enc.DecodeVal(asbytes, &asvg, true); err != nil {
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
	return c.commit(setKeys, vgs, delKeys)
}

func (c *rcache) commit(eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) error {
	// now we have all the deltas, we can set them
	for i, ek := range eks {
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry, clock.New())
		if !alive {
			c.cache.Del(ek)
		} else {
			buf := make([]byte, c.enc.ValLenHint(vgs[i]))
			n, err := c.enc.EncodeVal(buf, vgs[i])
			if err != nil {
				return err
			}
			buf = buf[:n]
			c.cache.SetWithTTL(ek, buf, int64(len(ek)+cap(buf)), ttl)
		}
	}
	for _, k := range delks {
		c.cache.Del(k)
	}
	return nil
}
