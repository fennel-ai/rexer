package cache

import (
	"fennel/hangar"
	"fennel/lib/ftypes"
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
	reqchan := make(chan getRequest, parallelism)
	ret := rcache{
		planeID: planeId,
		cache:   cache,
		reqchan: reqchan,
		enc:     enc,
	}
	// spin up lots of goroutines to handle requests in parallel
	for i := 0; i < parallelism; i++ {
		go ret.respond(reqchan)
	}
	return &ret, nil
}

type rcache struct {
	enc     hangar.Encoder
	planeID ftypes.RealmID
	cache   *ristretto.Cache
	reqchan chan getRequest
}

func (c *rcache) Restore(source io.Reader) error {
	panic("implement me")
}

func (c *rcache) Teardown() error {
	if !test.IsInTest() {
		return fmt.Errorf("can not teardown a store outside of test mode")
	}
	return c.Close()
}

func (c *rcache) Backup(sink io.Writer, since uint64) (uint64, error) {
	return 0, fmt.Errorf("can not backup a cache store")
}

func (c *rcache) Encoder() hangar.Encoder {
	return c.enc
}

func (c *rcache) Close() error {
	c.cache.Close()
	return nil
}

func (c *rcache) PlaneID() ftypes.RealmID {
	return c.planeID
}

// GetMany returns the values for the given keyGroups.
// It parallelizes the requests to the underlying cache upto a degree of parallelism
func (c *rcache) GetMany(keys []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	// we try to spread across available workers while giving each worker
	// a minimum of DB_BATCH_SIZE keyGroups to work on
	batch := len(keys) / parallelism
	if batch < batchSize {
		batch = batchSize
	}
	chans := make([]chan []hangar.Result, 0, len(keys)/batch)
	for i := 0; i < len(keys); i += batch {
		end := i + batch
		if end > len(keys) {
			end = len(keys)
		}
		resch := make(chan []hangar.Result, 1)
		chans = append(chans, resch)
		c.reqchan <- getRequest{
			keyGroups: keys[i:end],
			resch:     resch,
		}
	}
	results := make([]hangar.ValGroup, 0, len(keys))
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
func (c *rcache) SetMany(keys []hangar.Key, deltas []hangar.ValGroup) error {
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
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

func (c *rcache) DelMany(keyGroups []hangar.KeyGroup) error {
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
	evs, err := hangar.EncodeValMany(vgs, c.enc)
	if err != nil {
		return err
	}
	// now we have all the deltas, we can set them
	for i, ek := range eks {
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
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

var _ hangar.Hangar = &rcache{}

type getRequest struct {
	keyGroups []hangar.KeyGroup
	resch     chan<- []hangar.Result
}

func (c *rcache) respond(reqchan chan getRequest) {
	for {
		req := <-reqchan
		res := make([]hangar.Result, len(req.keyGroups))
		eks, err := hangar.EncodeKeyManyKG(req.keyGroups, c.enc)
		if err != nil {
			for i := range res {
				res[i] = hangar.Result{
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
				if req.keyGroups[i].Fields.IsPresent() {
					res[i].Ok.Select(req.keyGroups[i].Fields.MustGet())
				}
			}
		}
		req.resch <- res
	}
}
