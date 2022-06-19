package db

import (
	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/test"
	"fmt"
	"io"
	"os"

	"github.com/dgraph-io/badger/v3"
)

const (
	PARALLELISM   = 256
	DB_BATCH_SIZE = 64
)

type badgerDB struct {
	planeID  ftypes.RealmID
	baseOpts badger.Options
	enc      hangar.Encoder
	db       *badger.DB
	reqchan  chan getRequest
}

func (b *badgerDB) Restore(source io.Reader) error {
	panic("implement me")
}

func (b *badgerDB) Teardown() error {
	if !test.IsInTest() {
		return fmt.Errorf("can not teardown a store outside of tests")
	}
	if err := b.Close(); err != nil {
		return err
	}
	return os.Remove(b.baseOpts.Dir)
}

func (b *badgerDB) Backup(sink io.Writer, since uint64) (uint64, error) {
	return b.db.Backup(sink, since)
}

func (b *badgerDB) Close() error {
	return b.db.Close()
}

type getRequest struct {
	keyGroups []hangar.KeyGroup
	resch     chan<- []hangar.Result
}

func NewHangar(planeID ftypes.RealmID, dirname string, blockCacheBytes int64, enc hangar.Encoder) (*badgerDB, error) {
	opts := badger.DefaultOptions(dirname)
	opts = opts.WithLoggingLevel(badger.WARNING)
	opts = opts.WithBlockCacheSize(blockCacheBytes)
	reqchan := make(chan getRequest)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	bs := badgerDB{
		planeID:  planeID,
		baseOpts: opts,
		db:       db,
		reqchan:  reqchan,
		enc:      enc,
	}
	// spin up lots of goroutines to handle requests in parallel
	for i := 0; i < PARALLELISM; i++ {
		go bs.respond(reqchan)
	}
	return &bs, nil
}

func (b *badgerDB) PlaneID() ftypes.RealmID {
	return b.planeID
}

func (b *badgerDB) Encoder() hangar.Encoder {
	return b.enc
}

// GetMany returns the values for the given keyGroups.
// It parallelizes the requests to the underlying DB upto a degree of PARALLELISM
func (b *badgerDB) GetMany(kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	// we try to spread across available workers while giving each worker
	// a minimum of DB_BATCH_SIZE keyGroups to work on
	batch := len(kgs) / PARALLELISM
	if batch < DB_BATCH_SIZE {
		batch = DB_BATCH_SIZE
	}
	chans := make([]chan []hangar.Result, 0, len(kgs)/batch)
	for i := 0; i < len(kgs); i += batch {
		end := i + batch
		if end > len(kgs) {
			end = len(kgs)
		}
		resch := make(chan []hangar.Result, 1)
		chans = append(chans, resch)
		b.reqchan <- getRequest{
			keyGroups: kgs[i:end],
			resch:     resch,
		}
	}
	results := make([]hangar.ValGroup, 0, len(kgs))
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
// NOTE: the calculation of "deltas" isn't done as part of write transaction and so this
// assumes that the same keyGroups are not being written to in a separate goroutine.
func (b *badgerDB) SetMany(keys []hangar.Key, deltas []hangar.ValGroup) error {
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
	}
	eks, err := hangar.EncodeKeyMany(keys, b.enc)
	if err != nil {
		return err
	}
	// since we may only be setting some indices of the keyGroups, we need to
	// read existing deltas, merge them, and get the full deltas to be written
	err = b.db.View(func(txn *badger.Txn) error {
		for i, ek := range eks {
			var old hangar.ValGroup
			olditem, err := txn.Get(ek)
			switch err {
			case badger.ErrKeyNotFound:
				// no existing value, so just use the deltas
				old = deltas[i]
			case nil:
				// existing value, so merge it with the deltas
				if err = olditem.Value(func(val []byte) error {
					_, err := b.enc.DecodeVal(val, &old, false)
					return err
				}); err != nil {
					return err
				}
				if err = old.Update(deltas[i]); err != nil {
					return err
				}
			default: // some other error
				return err
			}
			deltas[i] = old
		}
		return nil
	})
	if err != nil {
		return err
	}
	return b.commit(eks, deltas, nil)
}

func (b *badgerDB) DelMany(keyGroups []hangar.KeyGroup) error {
	eks, err := hangar.EncodeKeyManyKG(keyGroups, b.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]hangar.ValGroup, 0, len(keyGroups))
	delKeys := make([][]byte, 0, len(keyGroups))
	err = b.db.View(func(txn *badger.Txn) error {
		for i, ek := range eks {
			var old hangar.ValGroup
			olditem, err := txn.Get(ek)
			switch err {
			case badger.ErrKeyNotFound:
				// no existing value, so nothing to set/delete
				continue
			case nil:
				if err := olditem.Value(func(val []byte) error {
					if _, err := b.enc.DecodeVal(val, &old, false); err != nil {
						return err
					}
					if keyGroups[i].Fields.IsAbsent() {
						delKeys = append(delKeys, ek)
					} else {
						old.Del(keyGroups[i].Fields.MustGet())
						if len(old.Fields) > 0 {
							setKeys = append(setKeys, ek)
							vgs = append(vgs, old)
						} else {
							delKeys = append(delKeys, ek)
						}
					}
					return nil
				}); err != nil {
					return err
				}
			default:
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return b.commit(setKeys, vgs, delKeys)
}

func (b *badgerDB) respond(reqchan chan getRequest) {
	for {
		req := <-reqchan
		res := make([]hangar.Result, len(req.keyGroups))
		eks, err := hangar.EncodeKeyManyKG(req.keyGroups, b.enc)
		if err != nil {
			for i := range res {
				res[i].Err = err
			}
			req.resch <- res
			continue
		}
		b.db.View(func(txn *badger.Txn) error {
			for i, ek := range eks {
				item, err := txn.Get(ek)
				switch err {
				case badger.ErrKeyNotFound:
				case nil:
					if err := item.Value(func(val []byte) error {
						if _, err := b.enc.DecodeVal(val, &res[i].Ok, false); err != nil {
							return err
						}
						if req.keyGroups[i].Fields.IsPresent() {
							res[i].Ok.Select(req.keyGroups[i].Fields.MustGet())
						}
						return nil
					}); err != nil {
						res[i].Err = err
					}
				default:
					res[i].Err = err
				}
			}
			return nil
		})
		req.resch <- res
	}
}

func (b *badgerDB) commit(eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) error {
	evs, err := hangar.EncodeValMany(vgs, b.enc)
	if err != nil {
		return err
	}
	wb := b.db.NewWriteBatch()
	defer wb.Cancel()
	for i, ek := range eks {
		e := badger.NewEntry(ek, evs[i])
		// if ttl is 0, we set the key to never expire, else we set it to expire in ttl duration from now
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			// if key is not alive, we delete it for good, just to be safe
			if err := wb.Delete(ek); err != nil {
				return err
			}
		} else {
			if ttl != 0 {
				e = e.WithTTL(ttl)
			}
			if err := wb.SetEntry(e); err != nil {
				return err
			}
		}
	}
	for _, k := range delks {
		if err := wb.Delete(k); err != nil {
			return err
		}
	}
	return wb.Flush()
}

var _ hangar.Hangar = &badgerDB{}
