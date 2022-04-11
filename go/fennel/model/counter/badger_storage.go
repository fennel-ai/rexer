package counter

import (
	"context"
	"fennel/fbadger"
	"fennel/lib/codex"
	"fennel/lib/timer"
	"fmt"

	"fennel/lib/ftypes"
	"fennel/lib/utils/binary"
	"fennel/lib/value"
	"fennel/tier"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

const (
	tablet = fbadger.Aggregate
	// defaultCodec key design is: groupkey | width | index | aggregate_name, where window is one of the standard windows
	// value is json encoded value
	defaultCodec codex.Codex = 1
)

type BadgerStorage struct{}

func (b BadgerStorage) GetBucketStore() BucketStore {
	// TODO implement me
	panic("implement me")
}

func (b BadgerStorage) Get(ctx context.Context, tier tier.Tier, name ftypes.AggName, buckets []Bucket, default_ value.Value) ([]value.Value, error) {
	ret, err := b.GetMulti(ctx, tier, []ftypes.AggName{name}, [][]Bucket{buckets}, []value.Value{default_})
	if err != nil {
		return nil, err
	}
	return ret[0], nil
}

func (b BadgerStorage) GetMulti(ctx context.Context, tier tier.Tier, names []ftypes.AggName, buckets [][]Bucket, defaults_ []value.Value) ([][]value.Value, error) {
	defer timer.Start(ctx, tier.ID, "badger_flat_storage.get_multi").Stop()
	if len(names) != len(buckets) || len(names) != len(defaults_) {
		return nil, fmt.Errorf("badger_storage.GetMulti: names, buckets, and defaults must be the same length")
	}
	if len(names) == 0 {
		return nil, nil
	}
	ret := make([][]value.Value, len(names))
	for i := range buckets {
		ret[i] = make([]value.Value, len(buckets[i]))
	}
	err := tier.Badger.View(func(txn *badger.Txn) error {
		for i := range names {
			for j := range buckets[i] {
				key, err := badgerEncode(names[i], buckets[i][j])
				if err != nil {
					return err
				}
				item, err := txn.Get(key)
				switch err {
				case badger.ErrKeyNotFound:
					ret[i][j] = defaults_[i]
				case nil:
					item.Value(func(val []byte) error {
						if ret[i][j], err = value.FromJSON(val); err != nil {
							return err
						}
						return nil
					})
				default:
					return err
				}
			}
		}
		return nil
	})
	return ret, err
}

func (b BadgerStorage) SetMulti(ctx context.Context, tier tier.Tier, names []ftypes.AggName, deltas [][]Bucket) error {
	defer timer.Start(ctx, tier.ID, "badger_flat_storage.set_multi").Stop()
	if len(names) != len(deltas) {
		return fmt.Errorf("badger_storage.SetMulti: names, deltas must be the same length")
	}
	if len(names) == 0 {
		return nil
	}
	return tier.Badger.Update(func(txn *badger.Txn) error {
		for i, name := range names {
			for _, bucket := range deltas[i] {
				k, err := badgerEncode(name, bucket)
				if err != nil {
					return err
				}
				v, err := bucket.Value.MarshalJSON()
				if err != nil {
					return err
				}
				if err := txn.Set(k, v); err != nil {
					return err
				}
			}
		}
		// no error so far, so transaction will be committed
		// add logging just before this
		for i, name := range names {
			tier.Logger.Info("Updating badger keys for aggregate",
				zap.String("aggregate", string(name)),
				zap.Int("num_keys", len(deltas[i])),
			)
		}
		return nil
	})
}

func (b BadgerStorage) Set(ctx context.Context, tier tier.Tier, name ftypes.AggName, deltas []Bucket) error {
	return b.SetMulti(ctx, tier, []ftypes.AggName{name}, [][]Bucket{deltas})
}

var _ BucketStore = BadgerStorage{}

// defaultCodec key design is: tablet | codex | groupkey | window | width | index | aggregate_name
func badgerEncode(name ftypes.AggName, bucket Bucket) ([]byte, error) {
	buf := make([]byte, 2+8+len(bucket.Key)+8+8+8+8+len(name))
	cur := 0
	if n, err := tablet.Write(buf[cur:]); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := defaultCodec.Write(buf[cur:]); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutString(buf[cur:], bucket.Key); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], uint64(bucket.Window)); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], bucket.Width); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutUvarint(buf[cur:], bucket.Index); err != nil {
		return nil, err
	} else {
		cur += n
	}
	if n, err := binary.PutString(buf[cur:], string(name)); err != nil {
		return nil, err
	} else {
		cur += n
	}
	return buf[:cur], nil
}

func badgerDecode(buf []byte) (ftypes.AggName, Bucket, error) {
	cur := 0
	tbl, n, err := fbadger.ReadTablet(buf)
	if err != nil {
		return "", Bucket{}, err
	}
	if tbl != tablet {
		return "", Bucket{}, fmt.Errorf("badgerDecode: invalid tablet: %v", tbl)
	}
	cur += n

	codec, n, err := codex.Read(buf[cur:])
	if err != nil {
		return "", Bucket{}, err
	}
	if codec != defaultCodec {
		return "", Bucket{}, fmt.Errorf("badgerDecode: invalid codec: %v", codec)
	}
	cur += n

	key, n, err := binary.ReadString(buf[cur:])
	if err != nil {
		return "", Bucket{}, err
	} else {
		cur += n
	}
	window, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return "", Bucket{}, err
	} else {
		cur += n
	}
	width, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return "", Bucket{}, err
	} else {
		cur += n
	}
	index, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return "", Bucket{}, err
	} else {
		cur += n
	}
	name, n, err := binary.ReadString(buf[cur:])
	if err != nil {
		return "", Bucket{}, err
	} else {
		cur += n
	}
	return ftypes.AggName(name), Bucket{Key: key, Window: ftypes.Window(window), Width: width, Index: index}, nil
}
