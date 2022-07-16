package db

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"fennel/hangar"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/nitrous/backup"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

const (
	PARALLELISM   = 256
	DB_BATCH_SIZE = 64
)

type badgerDB struct {
	planeID       ftypes.RealmID
	baseOpts      badger.Options
	enc           hangar.Encoder
	db            *badger.DB
	reqchan       chan getRequest
	backupManager *backup.BackupManager
	logger        *zap.Logger

	// WaitGroup to wait for all goroutines to finish.
	wg *sync.WaitGroup
}

func (b *badgerDB) Restore(dbDir string) error {
	backups, err := b.backupManager.ListBackups()
	if err != nil {
		return err
	}
	if len(backups) == 0 {
		b.logger.Warn("There is no previous backups")
		return nil
	}
	sort.Strings(backups)
	backupToRecover := backups[len(backups)-1]
	b.logger.Info(fmt.Sprintf("Going to restorethe lastest backup: %s", backupToRecover))
	err = b.backupManager.RestoreToPath(dbDir, backupToRecover)
	if err != nil {
		return err
	}
	b.logger.Info("Successfully restored the latest backup")
	return nil
}

func (b *badgerDB) Teardown() error {
	if err := b.Close(); err != nil {
		return err
	}
	return os.Remove(b.baseOpts.Dir)
}

func (b *badgerDB) BackupManager(_ io.Writer, _ uint64) (uint64, error) {

}

func (b *badgerDB) Backup(_ io.Writer, _ uint64) (uint64, error) {
	opt := b.db.Opts()
	err := b.db.Close()
	if err != nil {
		return 0, nil
	}

	err = b.backupManager.BackupPath(opt.Dir, time.Now().Format(time.RFC3339))
	if err != nil {
		return 0, nil
	}

	newDB, err := badger.Open(opt)
	if err != nil {
		return 0, nil
	}
	b.db = newDB
	return 0, nil
}

func (b *badgerDB) Close() error {
	// Close the request channel to signal to all read goroutines to stop.
	close(b.reqchan)
	// Wait for all read goroutines to finish.
	b.wg.Wait()
	return b.db.Close()
}

type getRequest struct {
	keyGroups []hangar.KeyGroup
	resch     chan<- []hangar.Result
}

func NewHangar(planeID ftypes.RealmID, dirname string, blockCacheBytes int64, enc hangar.Encoder, backupManager *backup.BackupManager) (*badgerDB, error) {
	opts := badger.DefaultOptions(dirname)
	opts = opts.WithLoggingLevel(badger.WARNING)
	opts = opts.WithBlockCacheSize(blockCacheBytes)
	reqchan := make(chan getRequest)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	bs := badgerDB{
		planeID:       planeID,
		baseOpts:      opts,
		db:            db,
		reqchan:       reqchan,
		enc:           enc,
		wg:            &sync.WaitGroup{},
		backupManager: backupManager,
	}
	// spin up lots of goroutines to handle requests in parallel
	bs.wg.Add(PARALLELISM)
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
func (b *badgerDB) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	_, t := timer.Start(ctx, b.planeID, "hangar.db.getmany")
	defer t.Stop()
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

func (b *badgerDB) respond(reqchan chan getRequest) {
	defer b.wg.Done()
	for req := range reqchan {
		res := make([]hangar.Result, len(req.keyGroups))
		eks, err := hangar.EncodeKeyManyKG(req.keyGroups, b.enc)
		if err != nil {
			for i := range res {
				res[i].Err = err
			}
			req.resch <- res
			continue
		}
		_ = b.db.View(func(txn *badger.Txn) error {
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
