package cache

import (
	"fennel/lib/ftypes"
	"fennel/store"
	"fennel/store/db"
	"fennel/test"
	"fmt"
	"io"
	"log"

	"github.com/dgraph-io/ristretto"
)

const (
	// batchSize the size of the batches in which we break down
	// incoming get calls
	batchSize   = 1000
	parallelism = 64
)

func NewStore(planeId ftypes.RealmID, maxSize, avgSize uint64, enc store.Encoder) (rcache, error) {
	config := &ristretto.Config{
		BufferItems: 64,
		NumCounters: 10 * int64(maxSize/avgSize),
		MaxCost:     int64(maxSize),
		Metrics:     true,
	}
	cache, err := ristretto.NewCache(config)
	if err != nil {
		return rcache{}, err
	}
	reqchan := make(chan getRequest, db.PARALLELISM)
	ret := rcache{
		planeID: planeId,
		cache:   cache,
		reqchan: reqchan,
		enc:     enc,
	}
	// spin up lots of goroutines to handle requests in parallel
	for i := 0; i < db.PARALLELISM; i++ {
		go ret.respond(reqchan)
	}
	return ret, nil
}

type rcache struct {
	enc     store.Encoder
	planeID ftypes.RealmID
	cache   *ristretto.Cache
	reqchan chan getRequest
}

func (c rcache) Restore(source io.Reader) error {
	panic("implement me")
}

func (c rcache) Teardown() error {
	if !test.IsInTest() {
		return fmt.Errorf("can not teardown a store outside of test mode")
	}
	return c.Close()
}

func (c rcache) Backup(sink io.Writer, since uint64) (uint64, error) {
	return 0, fmt.Errorf("can not backup a cache store")
}

func (c rcache) Encoder() store.Encoder {
	return c.enc
}

func (c rcache) Close() error {
	c.cache.Close()
	return nil
}

func (c rcache) PlaneID() ftypes.RealmID {
	return c.planeID
}

// GetMany returns the values for the given keyGroups.
// It parallelizes the requests to the underlying cache upto a degree of parallelism
func (c rcache) GetMany(keys []store.KeyGroup) ([]store.ValGroup, error) {
	// we try to spread across available workers while giving each worker
	// a minimum of DB_BATCH_SIZE keyGroups to work on
	batch := len(keys) / parallelism
	if batch < batchSize {
		batch = batchSize
	}
	chans := make([]chan []store.Result, 0, len(keys)/batch)
	for i := 0; i < len(keys); i += batch {
		end := i + batch
		if end > len(keys) {
			end = len(keys)
		}
		resch := make(chan []store.Result, 1)
		chans = append(chans, resch)
		c.reqchan <- getRequest{
			keyGroups: keys[i:end],
			resch:     resch,
		}
	}
	results := make([]store.ValGroup, 0, len(keys))
	for _, ch := range chans {
		subresults := <-ch
		for _, res := range subresults {
			if res.Err != nil {
				return nil, res.Err
			}
			results = append(results, res.Ok)
		}
	}
	return results, nil
}

// SetMany sets many keyGroups in a single transaction. Since these are all set in a single
// transaction, there is no parallelism. If parallelism is desired, create batches of
// keyGroups and call SetMany on each batch.
func (c rcache) SetMany(keys []store.Key, deltas []store.ValGroup) error {
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
	}
	// since we may only be setting some indices of the keyGroups, we need to
	// read existing deltas, merge them, and get the full deltas to be written
	eks, err := store.EncodeKeyMany(keys, c.enc)
	if err != nil {
		return nil
	}
	for i, ek := range eks {
		var oldvg store.ValGroup
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

func (c rcache) DelMany(keyGroups []store.KeyGroup) error {
	eks, err := store.EncodeKeyManyKG(keyGroups, c.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]store.ValGroup, 0, len(keyGroups))
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
		var asvg store.ValGroup
		if _, err := c.enc.DecodeVal(asbytes, &asvg, true); err != nil {
			return err
		}
		asvg.Del(keyGroups[i].Fields)
		if len(asvg.Fields) > 0 {
			setKeys = append(setKeys, ek)
			vgs = append(vgs, asvg)
		} else {
			delKeys = append(delKeys, ek)
		}
	}
	return c.commit(setKeys, vgs, delKeys)
}

func (c rcache) commit(eks [][]byte, vgs []store.ValGroup, delks [][]byte) error {
	evs, err := store.EncodeValMany(vgs, c.enc)
	if err != nil {
		return err
	}
	// now we have all the deltas, we can set them
	for i, ek := range eks {
		ttl, alive := store.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			c.cache.Del(ek)
		} else {
			c.cache.SetWithTTL(ek, evs[i], int64(len(ek)+len(evs[i])), ttl)
		}
	}
	for _, k := range delks {
		c.cache.Del(k)
	}
	return nil
}

var _ store.Store = &rcache{}

type getRequest struct {
	keyGroups []store.KeyGroup
	resch     chan<- []store.Result
}

func (c rcache) respond(reqchan chan getRequest) {
	for {
		req := <-reqchan
		res := make([]store.Result, len(req.keyGroups))
		eks, err := store.EncodeKeyManyKG(req.keyGroups, c.enc)
		if err != nil {
			for i := range res {
				res[i] = store.Result{
					Err: fmt.Errorf("error encoding key: %v", err),
				}
			}
			req.resch <- res
			continue
		}
		for i, ek := range eks {
			val, found := c.cache.Get(ek)
			if !found {
				// not found, this is not an error, so we will just return empty ValGroup
				continue
			}
			if asbytes, ok := val.([]byte); !ok {
				log.Printf("Cache: expected []byte, got %T", val)
				res[i].Err = fmt.Errorf("cache: expected []byte, got %T", val)
			} else if _, err := c.enc.DecodeVal(asbytes, &res[i].Ok, true); err != nil {
				res[i].Err = err
			} else {
				res[i].Ok.Select(req.keyGroups[i].Fields)
			}
		}
		req.resch <- res
	}
}
