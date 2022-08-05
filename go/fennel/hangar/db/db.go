package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/parallel"

	"github.com/dgraph-io/badger/v3"
)

const (
	PARALLELISM   = 256
	DB_BATCH_SIZE = 64
)

type badgerDB struct {
	planeID    ftypes.RealmID
	baseOpts   badger.Options
	enc        hangar.Encoder
	db         *badger.DB
	workerPool *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
}

func (b *badgerDB) Restore(source io.Reader) error {
	panic("implement me")
}

func (b *badgerDB) Teardown() error {
	if err := b.Close(); err != nil {
		return err
	}
	return os.Remove(b.baseOpts.Dir)
}

func (b *badgerDB) Backup(sink io.Writer, since uint64) (uint64, error) {
	return b.db.Backup(sink, since)
}

func (b *badgerDB) Close() error {
	// Close the worker pool.
	b.workerPool.Close()
	return b.db.Close()
}

func NewHangar(planeID ftypes.RealmID, dirname string, blockCacheBytes int64, enc hangar.Encoder) (*badgerDB, error) {
	opts := badger.DefaultOptions(dirname)
	opts = opts.WithLoggingLevel(badger.WARNING)
	opts = opts.WithBlockCacheSize(blockCacheBytes)
	opts = opts.WithBlockSize(16 * 1024)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	bs := badgerDB{
		planeID:    planeID,
		baseOpts:   opts,
		db:         db,
		workerPool: parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup](PARALLELISM),
		enc:        enc,
	}
	// Start periodic GC of value log.
	go bs.runPeriodicGC()
	return &bs, nil
}

func (b *badgerDB) runPeriodicGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		discardRatio := float64(0.5)
		err := b.db.RunValueLogGC(discardRatio)
		if errors.Is(err, badger.ErrRejected) && b.db.IsClosed() {
			log.Printf("DB is closed, stopping value log GC")
			return
		} else if err != nil {
			log.Printf("badger value log GC failed: %v", err)
		}
	}
}

func (b *badgerDB) PlaneID() ftypes.RealmID {
	return b.planeID
}

func (b *badgerDB) Encoder() hangar.Encoder {
	return b.enc
}

// GetMany returns the values for the given keyGroups.
// It parallelizes the requests to the underlying DB upto a degree of PARALLELISM
func (b *badgerDB) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	_, t := timer.Start(ctx, b.planeID, "hangar.db.getmany")
	defer t.Stop()
	// We try to spread across available workers while giving each worker
	// a minimum of DB_BATCH_SIZE keyGroups to work on.
	batch := len(kgs) / PARALLELISM
	if batch < DB_BATCH_SIZE {
		batch = DB_BATCH_SIZE
	}
	return b.workerPool.Process(ctx, kgs, func(keyGroups []hangar.KeyGroup, valGroups []hangar.ValGroup) error {
		eks, err := hangar.EncodeKeyManyKG(keyGroups, b.enc)
		if err != nil {
			return fmt.Errorf("failed to encode keys: %w", err)
		}
		err = b.db.View(func(txn *badger.Txn) error {
			for i, ek := range eks {
				item, err := txn.Get(ek)
				switch err {
				case badger.ErrKeyNotFound:
				case nil:
					if err := item.Value(func(val []byte) error {
						if _, err := b.enc.DecodeVal(val, &valGroups[i], false); err != nil {
							return err
						}
						if keyGroups[i].Fields.IsPresent() {
							valGroups[i].Select(keyGroups[i].Fields.MustGet())
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
		return err
	}, batch)
}

// SetMany sets many keyGroups in a single transaction. Since these are all set in a single
// transaction, there is no parallelism. If parallelism is desired, create batches of
// keyGroups and call SetMany on each batch.
// NOTE: the calculation of "deltas" isn't done as part of write transaction and so this
// assumes that the same keyGroups are not being written to in a separate goroutine.
func (b *badgerDB) SetMany(ctx context.Context, keys []hangar.Key, deltas []hangar.ValGroup) error {
	_, t := timer.Start(ctx, b.planeID, "hangar.db.setmany")
	defer t.Stop()
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
	}
	// Consolidate updates to fields in the same key.
	keys, deltas, err := hangar.MergeUpdates(keys, deltas)
	if err != nil {
		return fmt.Errorf("failed to merge updates: %w", err)
	}
	eks, err := hangar.EncodeKeyMany(keys, b.enc)
	if err != nil {
		return err
	}
	// since we may only be setting some indices of the keyGroups, we need to
	// read existing deltas, merge them, and get the full deltas to be written
	for {
		err = b.db.Update(func(txn *badger.Txn) error {
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
			return b.write(txn, eks, deltas, nil)
		})
		if errors.Is(err, badger.ErrConflict) {
			log.Print("badgerDB: conflict detected, retrying")
			continue
		}
		break
	}
	return err
}

func (b *badgerDB) DelMany(ctx context.Context, keyGroups []hangar.KeyGroup) error {
	_, t := timer.Start(ctx, b.planeID, "hangar.db.delmany")
	defer t.Stop()
	eks, err := hangar.EncodeKeyManyKG(keyGroups, b.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]hangar.ValGroup, 0, len(keyGroups))
	delKeys := make([][]byte, 0, len(keyGroups))
	err = b.db.Update(func(txn *badger.Txn) error {
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
		return b.write(txn, setKeys, vgs, delKeys)
	})
	return err
}

func (b *badgerDB) write(txn *badger.Txn, eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) error {
	evs, err := hangar.EncodeValMany(vgs, b.enc)
	if err != nil {
		return err
	}
	for i, ek := range eks {
		e := badger.NewEntry(ek, evs[i])
		// if ttl is 0, we set the key to never expire, else we set it to expire in ttl duration from now
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			// if key is not alive, we delete it for good, just to be safe
			if err := txn.Delete(ek); err != nil {
				return err
			}
		} else {
			if ttl != 0 {
				e = e.WithTTL(ttl)
			}
			if err := txn.SetEntry(e); err != nil {
				return err
			}
		}
	}
	for _, k := range delks {
		if err := txn.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

var _ hangar.Hangar = &badgerDB{}
