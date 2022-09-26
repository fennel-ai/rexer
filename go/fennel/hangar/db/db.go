package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sync"
	"time"

	"fennel/hangar"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/parallel"
	"github.com/dgraph-io/badger/v3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

const (
	READ_PARALLELISM  = 64
	WRITE_PARALLELISM = 64
	DB_BATCH_SIZE     = 32
)

type badgerDB struct {
	planeID      ftypes.RealmID
	opts         badger.Options
	enc          hangar.Encoder
	db           *badger.DB
	readWorkers  *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	writeWorkers *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	closeWg    sync.WaitGroup
	closeCh    chan int
}

func (b *badgerDB) Flush() error {
	//TODO implement me
	panic("implement me")
}

func (b *badgerDB) Teardown() error {
	if err := b.Close(); err != nil {
		return err
	}
	return os.Remove(b.opts.Dir)
}

func (b *badgerDB) Backup(_ io.Writer, _ uint64) (uint64, error) {
	panic("not implemented")
}

func (b *badgerDB) Close() error {
	// Close the worker pool and all other goroutines
	close(b.closeCh)
	b.readWorkers.Close()
	b.writeWorkers.Close()
	b.closeWg.Wait()
	return b.db.Close()
}

func NewHangar(planeID ftypes.RealmID, opts badger.Options, enc hangar.Encoder) (*badgerDB, error) {
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	if err = db.VerifyChecksum(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to verify checksum of the badgerdb instance: %w", err)
	}
	zap.L().Info("Successfully opened badgerdb", zap.Uint64("max_data_version", db.MaxVersion()))

	bs := badgerDB{
		planeID:      planeID,
		opts:         opts,
		db:           db,
		readWorkers:  parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_db_read", READ_PARALLELISM),
		writeWorkers: parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_db_write", WRITE_PARALLELISM),
		enc:          enc,
		closeCh:    make(chan int),
	}
	// Start periodic GC of value log.
	bs.closeWg.Add(1)
	go bs.runPeriodicGC()

	return &bs, nil
}

func (b *badgerDB) runPeriodicGC() {
	defer b.closeWg.Done()
	interval := time.Hour
	// Important: Set initial interval to be a random number between 0 and 1 hour.
	// This is to ensure that all nitrous instances are not doing GC at the same time.
	rand.Seed(time.Now().UnixNano())
	timer := time.NewTimer(time.Second * time.Duration(interval.Seconds()*rand.Float64()))
	defer timer.Stop()
	for {
		select {
		case _, ok := <-b.closeCh:
			if !ok {
				zap.L().Info("PeriodicGC goroutine got closing signal, returning...")
				return
			}
		case <-timer.C:
			discardRatio := float64(0.5)
			zap.L().Info("Running badger value log GC with discard ratio", zap.Float64("ratio", discardRatio))
			err := b.db.RunValueLogGC(discardRatio)
			if errors.Is(err, badger.ErrRejected) && b.db.IsClosed() {
				zap.L().Info("DB is closed, stopping value log GC")
				return
			} else if errors.Is(err, badger.ErrNoRewrite) {
				zap.L().Info("Value log GC resulted in no rewrite")
			} else if err != nil {
				zap.L().Info("Value log GC failed: %v", zap.Error(err))
			}
			timer.Reset(interval)
		}
	}
}

func (b *badgerDB) PlaneID() ftypes.RealmID {
	return b.planeID
}

func (b *badgerDB) Encoder() hangar.Encoder {
	return b.enc
}

var (
	badger_view_num_keys = promauto.NewSummaryVec(prometheus.SummaryOpts{
		Name: "badger_view_num_keys",
		Help: "Number of keys read in a badger View txn",
		// Track quantiles within small error
		Objectives: map[float64]float64{
			0.25:  0.05,
			0.50:  0.05,
			0.75:  0.05,
			0.90:  0.05,
			0.95:  0.02,
			0.99:  0.01,
			0.999: 0.001,
		},
	}, []string{"mode"})
)

// GetMany returns the values for the given keyGroups.
// It parallelizes the requests to the underlying DB upto a degree of PARALLELISM
func (b *badgerDB) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	var pool *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	if hangar.IsWrite(ctx) {
		pool = b.writeWorkers
	} else {
		pool = b.readWorkers
	}
	ctx, t := timer.Start(ctx, b.planeID, fmt.Sprintf("hangar.db.getmany.%s", hangar.GetMode(ctx)))
	defer t.Stop()
	return pool.Process(ctx, kgs, func(keyGroups []hangar.KeyGroup, valGroups []hangar.ValGroup) error {
		_, t := timer.Start(ctx, b.planeID, fmt.Sprintf("hangar.db.getmany.batch.%s", hangar.GetMode(ctx)))
		defer t.Stop()
		eks, err := hangar.EncodeKeyManyKG(keyGroups, b.enc)
		if err != nil {
			return fmt.Errorf("failed to encode keys: %w", err)
		}
		badger_view_num_keys.WithLabelValues(hangar.GetMode(ctx).String()).Observe(float64(len(eks)))
		err = b.db.View(func(txn *badger.Txn) error {
			_, t := timer.Start(ctx, b.planeID, fmt.Sprintf("badger.view.latency.%s", hangar.GetMode(ctx)))
			defer t.Stop()
			for i, ek := range eks {
				_, t := timer.Start(ctx, b.planeID, fmt.Sprintf("badger.get.latency.%s", hangar.GetMode(ctx)))
				item, err := txn.Get(ek)
				t.Stop()
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
	}, DB_BATCH_SIZE)
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
			zap.L().Info("badgerdb: conflict detected, retrying")
			// Add random jitter to avoid cascading conflicts.
			d := time.Millisecond * time.Duration(1000*rand.Float64())
			time.Sleep(d)
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
			zap.L().Info("badgerdb: conflict detected, retrying")
			// Add random jitter to avoid cascading conflicts.
			d := time.Millisecond * time.Duration(1000*rand.Float64())
			time.Sleep(d)
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
