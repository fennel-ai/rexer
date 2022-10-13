package pebble

import (
	"context"
	"fmt"
	"io"
	"time"

	"fennel/hangar"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/parallel"

	"github.com/cockroachdb/pebble"
	"github.com/dgraph-io/badger/v3"
	"github.com/raulk/clock"
)

const (
	READ_PARALLELISM  = 64
	WRITE_PARALLELISM = 64
	DB_BATCH_SIZE     = 32
)

type pebbleDB struct {
	planeID ftypes.RealmID
	db      *pebble.DB
	enc     hangar.Encoder

	readWorkers  *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	writeWorkers *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
}

func (p *pebbleDB) StartCompaction() error {
	//TODO implement me
	panic("implement me")
}

func (p *pebbleDB) StopCompaction() error {
	//TODO implement me
	panic("implement me")
}

func (p *pebbleDB) Flush() error {
	//TODO implement me
	panic("implement me")
}

var _ hangar.Hangar = (*pebbleDB)(nil)

func NewHangar(planeID ftypes.RealmID, dirname string, opts *pebble.Options, enc hangar.Encoder) (*pebbleDB, error) {
	db, err := pebble.Open(dirname, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open: %w", err)
	}
	startReportingMetrics(db)
	return &pebbleDB{
		planeID:      planeID,
		db:           db,
		enc:          enc,
		readWorkers:  parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_pebble_read", READ_PARALLELISM),
		writeWorkers: parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_pebble_write", WRITE_PARALLELISM),
	}, nil
}

func startReportingMetrics(db *pebble.DB) {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			_, ok := <-ticker.C
			if !ok {
				break
			}
			// TODO: report metrics
		}
	}()
}

func (p *pebbleDB) PlaneID() ftypes.RealmID {
	return p.planeID
}

func (p *pebbleDB) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	var pool *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	if hangar.IsWrite(ctx) {
		pool = p.writeWorkers
	} else {
		pool = p.readWorkers
	}
	ctx, t := timer.Start(ctx, p.planeID, fmt.Sprintf("hangar.pebble.getmany.%s", hangar.GetMode(ctx)))
	defer t.Stop()
	return pool.Process(ctx, kgs, func(keyGroups []hangar.KeyGroup, valGroups []hangar.ValGroup) error {
		ctx, t := timer.Start(ctx, p.planeID, fmt.Sprintf("hangar.pebble.getmany.batch.%s", hangar.GetMode(ctx)))
		defer t.Stop()
		eks, err := hangar.EncodeKeyManyKG(keyGroups, p.enc)
		if err != nil {
			return fmt.Errorf("failed to encode keys: %w", err)
		}
		for i, ek := range eks {
			_, t := timer.Start(ctx, p.planeID, fmt.Sprintf("hangar.pebble.get.latency.%s", hangar.GetMode(ctx)))
			item, closer, err := p.db.Get(ek)
			t.Stop()
			switch err {
			case pebble.ErrNotFound:
			case nil:
				defer closer.Close()
				if _, err := p.enc.DecodeVal(item, &valGroups[i], false); err != nil {
					return err
				}
				if keyGroups[i].Fields.IsPresent() {
					valGroups[i].Select(keyGroups[i].Fields.MustGet())
				}
			default:
				return err
			}
		}
		return nil
	}, DB_BATCH_SIZE)
}

func (p *pebbleDB) SetMany(ctx context.Context, keys []hangar.Key, deltas []hangar.ValGroup) error {
	_, t := timer.Start(ctx, p.planeID, "hangar.pebble.setmany")
	defer t.Stop()
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
	}
	// Consolidate updates to fields in the same key.
	keys, deltas, err := hangar.MergeUpdates(keys, deltas)
	if err != nil {
		return fmt.Errorf("failed to merge updates: %w", err)
	}
	eks, err := hangar.EncodeKeyMany(keys, p.enc)
	if err != nil {
		return err
	}
	for i, ek := range eks {
		var old hangar.ValGroup
		olditem, closer, err := p.db.Get(ek)
		switch err {
		case pebble.ErrNotFound:
			// no existing value, so just use the deltas
			old = deltas[i]
		case nil:
			defer closer.Close()
			// existing value, so merge it with the deltas
			if _, err := p.enc.DecodeVal(olditem, &old, false); err != nil {
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
	return p.write(eks, deltas, nil)
}

func (p *pebbleDB) DelMany(ctx context.Context, keyGroups []hangar.KeyGroup) error {
	_, t := timer.Start(ctx, p.planeID, "hangar.pebble.delmany")
	defer t.Stop()
	eks, err := hangar.EncodeKeyManyKG(keyGroups, p.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]hangar.ValGroup, 0, len(keyGroups))
	delKeys := make([][]byte, 0, len(keyGroups))
	// We create a managed transaction because we want to perform some cleanup
	// after the transaction is done executing.
	for i, ek := range eks {
		var old hangar.ValGroup
		olditem, closer, err := p.db.Get(ek)
		switch err {
		case badger.ErrKeyNotFound:
			// no existing value, so nothing to set/delete
			continue
		case nil:
			defer closer.Close()
			if _, err := p.enc.DecodeVal(olditem, &old, false); err != nil {
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
		default:
			return err
		}
	}
	return p.write(setKeys, vgs, delKeys)
}

func (p *pebbleDB) Close() error {
	return p.db.Close()
}

func (p *pebbleDB) Teardown() error {
	// TODO: implement
	return nil
}

func (p *pebbleDB) Backup(sink io.Writer, since uint64) (uint64, error) {
	return 0, nil
}

func (p *pebbleDB) Restore(source io.Reader) error {
	return nil
}

func (p *pebbleDB) write(eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) error {
	batch := p.db.NewBatch()
	defer batch.Close()
	allocated := arena.Bytes2D.Alloc(len(eks), len(eks))
	defer func() {
		for _, buf := range allocated {
			arena.Bytes.Free(buf)
		}
		arena.Bytes2D.Free(allocated)
	}()
	for i, ek := range eks {
		// if ttl is 0, we set the key to never expire, else we set it to expire in ttl duration from now
		_, alive := hangar.ExpiryToTTL(vgs[i].Expiry, clock.New())
		if !alive {
			// if key is not alive, we delete it for good, just to be safe
			err := batch.Delete(ek, pebble.Sync)
			if err != nil {
				return err
			}
		} else {
			sz := p.enc.ValLenHint(vgs[i])
			buf := arena.Bytes.Alloc(sz, sz)
			allocated = append(allocated, buf)
			n, err := p.enc.EncodeVal(buf, vgs[i])
			if err != nil {
				return fmt.Errorf("failed to encode value: %w", err)
			}
			buf = buf[:n]
			// TODO: handle ttl in offline/backup process.
			// if ttl != 0 {
			// 	e = e.WithTTL(ttl)
			// }
			if err := batch.Set(ek, buf, pebble.Sync); err != nil {
				return fmt.Errorf("failed to set entry: %w", err)
			}
		}
	}
	for _, k := range delks {
		if err := batch.Delete(k, pebble.Sync); err != nil {
			return fmt.Errorf("failed to delete key: %w", err)
		}
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	return nil
}
