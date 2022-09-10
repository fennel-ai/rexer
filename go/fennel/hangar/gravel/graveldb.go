package gravel

import (
	"context"
	"fennel/gravel"
	"fennel/hangar"
	"fennel/lib/arena"
	"fennel/lib/ftypes"
	"fennel/lib/timer"
	"fennel/lib/utils/parallel"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/detailyang/fastrand-go"
)

const (
	DB_BATCH_SIZE = 32
)

var (
	READ_PARALLELISM  = 2 * runtime.NumCPU()
	WRITE_PARALLELISM = runtime.NumCPU()
)

type gravelDb struct {
	planeID ftypes.RealmID
	db      *gravel.Gravel
	enc     hangar.Encoder

	readWorkers  *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	writeWorkers *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
}

func NewHangar(planeID ftypes.RealmID, dirname string, opts *gravel.Options, enc hangar.Encoder) (*gravelDb, error) {
	popts := (*opts).WithDirname(dirname)
	db, err := gravel.Open(popts)
	if err != nil {
		return nil, fmt.Errorf("failed to open: %w", err)
	}
	startReportingMetrics(db)
	return &gravelDb{
		planeID:      planeID,
		db:           db,
		enc:          enc,
		readWorkers:  parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_gravel_read", 4*READ_PARALLELISM),
		writeWorkers: parallel.NewWorkerPool[hangar.KeyGroup, hangar.ValGroup]("hangar_gravel_write", WRITE_PARALLELISM),
	}, nil
}

func startReportingMetrics(db *gravel.Gravel) {
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

func (g *gravelDb) PlaneID() ftypes.RealmID {
	return g.planeID
}

func (g *gravelDb) GetMany(ctx context.Context, kgs []hangar.KeyGroup) ([]hangar.ValGroup, error) {
	var pool *parallel.WorkerPool[hangar.KeyGroup, hangar.ValGroup]
	if hangar.IsWrite(ctx) {
		pool = g.writeWorkers
	} else {
		pool = g.readWorkers
	}
	sample := shouldSample()
	ctx, t := timer.Start(ctx, g.planeID, fmt.Sprintf("hangar.gravel.getmany.%s", hangar.GetMode(ctx)))
	defer t.Stop()
	return pool.Process(ctx, kgs, func(keyGroups []hangar.KeyGroup, valGroups []hangar.ValGroup) error {
		if sample {
			ctxNew, t := timer.Start(ctx, g.planeID, fmt.Sprintf("hangar.gravel.getmany.batch.%s", hangar.GetMode(ctx)))
			defer t.Stop()
			ctx = ctxNew
		}
		eks, err := hangar.EncodeKeyManyKG(keyGroups, g.enc)
		if err != nil {
			return fmt.Errorf("failed to encode keys: %w", err)
		}
		for i, ek := range eks {
			var item []byte
			if sample {
				_, t := timer.Start(ctx, g.planeID, fmt.Sprintf("hangar.gravel.get.latency.%s", hangar.GetMode(ctx)))
				item, err = g.db.Get(ek)
				t.Stop()
			} else {
				item, err = g.db.Get(ek)
			}
			switch err {
			case gravel.ErrNotFound:
			case nil:
				if _, err := g.enc.DecodeVal(item, &valGroups[i], false); err != nil {
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

func (g *gravelDb) SetMany(ctx context.Context, keys []hangar.Key, deltas []hangar.ValGroup) error {
	_, t := timer.Start(ctx, g.planeID, "hangar.gravel.setmany")
	defer t.Stop()
	if len(keys) != len(deltas) {
		return fmt.Errorf("key, value lengths do not match")
	}
	// Consolidate updates to fields in the same key.
	keys, deltas, err := hangar.MergeUpdates(keys, deltas)
	if err != nil {
		return fmt.Errorf("failed to merge updates: %w", err)
	}
	eks, err := hangar.EncodeKeyMany(keys, g.enc)
	if err != nil {
		return err
	}
	for i, ek := range eks {
		var old hangar.ValGroup
		olditem, err := g.db.Get(ek)
		switch err {
		case gravel.ErrNotFound:
			// no existing value, so just use the deltas
			old = deltas[i]
		case nil:
			// existing value, so merge it with the deltas
			if _, err := g.enc.DecodeVal(olditem, &old, false); err != nil {
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
	return g.write(eks, deltas, nil)
}

func (g *gravelDb) DelMany(ctx context.Context, keyGroups []hangar.KeyGroup) error {
	_, t := timer.Start(ctx, g.planeID, "hangar.gravel.delmany")
	defer t.Stop()
	eks, err := hangar.EncodeKeyManyKG(keyGroups, g.enc)
	if err != nil {
		return err
	}
	setKeys := make([][]byte, 0, len(keyGroups))
	vgs := make([]hangar.ValGroup, 0, len(keyGroups))
	delKeys := make([][]byte, 0, len(keyGroups))
	for i, ek := range eks {
		var old hangar.ValGroup
		olditem, err := g.db.Get(ek)
		switch err {
		case gravel.ErrNotFound:
			// no existing value, so nothing to set/delete
			continue
		case nil:
			if _, err := g.enc.DecodeVal(olditem, &old, false); err != nil {
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
	return g.write(setKeys, vgs, delKeys)
}

func (g *gravelDb) Close() error {
	return g.db.Close()
}

func (g *gravelDb) Teardown() error {
	return g.db.Teardown()
}

func (g gravelDb) Backup(sink io.Writer, since uint64) (uint64, error) {
	// TODO implement me
	panic("implement me")
}

func (g gravelDb) Restore(source io.Reader) error {
	// TODO implement me
	panic("implement me")
}

var _ hangar.Hangar = (*gravelDb)(nil)

func (g *gravelDb) write(eks [][]byte, vgs []hangar.ValGroup, delks [][]byte) error {
	batch := g.db.NewBatch()
	defer batch.Discard()
	allocated := arena.Bytes2D.Alloc(len(eks), len(eks))
	defer func() {
		for _, buf := range allocated {
			arena.Bytes.Free(buf)
		}
		arena.Bytes2D.Free(allocated)
	}()
	for i, ek := range eks {
		// if ttl is 0, we set the key to never expire, else we set it to expire in ttl duration from now
		_, alive := hangar.ExpiryToTTL(vgs[i].Expiry)
		if !alive {
			// if key is not alive, we delete it for good, just to be safe
			err := batch.Del(ek)
			if err != nil {
				return err
			}
		} else {
			sz := g.enc.ValLenHint(vgs[i])
			buf := arena.Bytes.Alloc(sz, sz)
			allocated = append(allocated, buf)
			n, err := g.enc.EncodeVal(buf, vgs[i])
			if err != nil {
				return fmt.Errorf("failed to encode value: %w", err)
			}
			buf = buf[:n]
			if err := batch.Set(ek, buf, uint32(vgs[i].Expiry)); err != nil {
				return fmt.Errorf("failed to set entry: %w", err)
			}
		}
	}
	for _, k := range delks {
		if err := batch.Del(k); err != nil {
			return fmt.Errorf("failed to delete key: %w", err)
		}
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("failed to commit batch: %w", err)
	}
	return nil
}

func shouldSample() bool {
	return (fastrand.FastRand() & ((1 << 7) - 1)) == 0
}
