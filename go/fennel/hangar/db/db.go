package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"fennel/hangar"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/parallel"

	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/options"
	"go.uber.org/zap"
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
	opts = opts.WithLogger(NewLogger(zap.L()))
	opts = opts.WithValueThreshold(1 << 10 /* 1 KB */)
	opts = opts.WithCompression(options.ZSTD)
	opts = opts.WithBlockSize(4 * 1024)
	opts = opts.WithNumCompactors(2)
	opts = opts.WithCompactL0OnClose(true)
	opts = opts.WithIndexCacheSize(2 << 30 /* 2 GB */)
	opts = opts.WithMemTableSize(1 << 30 /* 1 GB */)
	opts = opts.WithBlockCacheSize(blockCacheBytes)
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
	interval := time.Hour
	// Important: Set inital interval to be a random number between 0 and 1 hour.
	// This is to ensure that all nitrous instances are not doing GC at the same time.
	rand.Seed(time.Now().UnixNano())
	timer := time.NewTimer(time.Second * time.Duration(interval.Seconds()*rand.Float64()))
	defer timer.Stop()
	for {
		<-timer.C
		discardRatio := float64(0.5)
		log.Printf("Running badger value log GC with discard ratio %f", discardRatio)
		err := b.db.RunValueLogGC(discardRatio)
		if errors.Is(err, badger.ErrRejected) && b.db.IsClosed() {
			log.Printf("DB is closed, stopping value log GC")
			return
		} else if errors.Is(err, badger.ErrNoRewrite) {
			log.Printf("Value log GC resulted in no rewrite")
		} else if err != nil {
			log.Printf("Value log GC failed: %v", err)
		}
		timer.Reset(interval)
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
		_, t := timer.Start(ctx, b.planeID, "hangar.db.getmany.batch")
		defer t.Stop()
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
		txn := b.db.NewTransaction(true)
		defer txn.Discard()
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
		allocated, err := b.write(txn, eks, deltas, nil)
		defer func() {
			for _, buf := range allocated {
				arena.Bytes.Free(buf)
			}
			arena.Bytes2D.Free(allocated)
		}()
		if err != nil {
			return err
		}
		err = txn.Commit()
		switch {
		case err == nil:
			return nil
		case errors.Is(err, badger.ErrConflict):
			log.Print("badgerDB: conflict detected, retrying")
			continue
		default:
			return err
		}
	}
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
	for {
		// We create a managed transaction because we want to perform some cleanup
		// after the transaction is done executing.
		txn := b.db.NewTransaction(true)
		defer txn.Discard()
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
		allocated, err := b.write(txn, setKeys, vgs, delKeys)
		defer func() {
			for _, buf := range allocated {
				arena.Bytes.Free(buf)
			}
			arena.Bytes2D.Free(allocated)
		}()
		if err != nil {
			return err
		}
		err = txn.Commit()
		switch {
		case err == nil:
			return nil
		case errors.Is(err, badger.ErrConflict):
			log.Print("badgerDB: conflict detected, retrying")
			continue
		default:
			return err
		}
	}
}

func (b *badgerDB) write(txn *badger.Txn, eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) ([][]byte, error) {
	allocated := arena.Bytes2D.Alloc(len(eks), len(eks))
	for i, ek := range eks {
		// if ttl is 0, we set the key to never expire, else we set it to expire in ttl duration from now
		ttl, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			// if key is not alive, we delete it for good, just to be safe
			if err := txn.Delete(ek); err != nil {
				return allocated, fmt.Errorf("failed to delete key: %w", err)
			}
		} else {
			sz := b.enc.ValLenHint(vgs[i])
			buf := arena.Bytes.Alloc(sz, sz)
			allocated = append(allocated, buf)
			n, err := b.enc.EncodeVal(buf, vgs[i])
			if err != nil {
				return allocated, fmt.Errorf("failed to encode value: %w", err)
			}
			buf = buf[:n]
			e := badger.NewEntry(ek, buf)
			if ttl != 0 {
				e = e.WithTTL(ttl)
			}
			if err := txn.SetEntry(e); err != nil {
				return allocated, fmt.Errorf("failed to set entry: %w", err)
			}
		}
	}
	for _, k := range delks {
		if err := txn.Delete(k); err != nil {
			return allocated, fmt.Errorf("failed to delete key: %w", err)
		}
	}
	return allocated, nil
}

var _ hangar.Hangar = &badgerDB{}
