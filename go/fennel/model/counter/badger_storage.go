package counter

import (
	"context"
	"fennel/lib/codex"
	"fennel/lib/counter"
	"fennel/lib/kvstore"
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
	tablet = kvstore.Aggregate
	// defaultCodec key design is: groupkey | width | index | aggregate_name, where window is one of the standard windows
	// value is json encoded value
	defaultCodec codex.Codex = 1
)

type BadgerStorage struct{}

func (b BadgerStorage) GetBucketStore() BucketStore {
	// TODO implement me
	panic("implement me")
}

func (b BadgerStorage) Get(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, buckets []counter.Bucket, default_ value.Value) ([]value.Value, error) {
	ret, err := b.GetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{buckets}, []value.Value{default_})
	if err != nil {
		return nil, err
	}
	return ret[0], nil
}

func (b BadgerStorage) GetMulti(ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, buckets [][]counter.Bucket, defaults_ []value.Value) ([][]value.Value, error) {
	defer timer.Start(ctx, tier.ID, "badger_flat_storage.get_multi").Stop()
	if len(aggIds) != len(buckets) || len(aggIds) != len(defaults_) {
		return nil, fmt.Errorf("badger_storage.GetMulti: names, buckets, and defaults must be the same length")
	}
	if len(aggIds) == 0 {
		return nil, nil
	}
	ret := make([][]value.Value, len(aggIds))
	for i := range buckets {
		ret[i] = make([]value.Value, len(buckets[i]))
	}
	err := tier.Badger.View(func(txn *badger.Txn) error {
		for i := range aggIds {
			for j := range buckets[i] {
				key, err := badgerEncode(aggIds[i], buckets[i][j])
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

func (b BadgerStorage) SetMulti(ctx context.Context, tier tier.Tier, aggIds []ftypes.AggId, deltas [][]counter.Bucket) error {
	defer timer.Start(ctx, tier.ID, "badger_flat_storage.set_multi").Stop()
	if len(aggIds) != len(deltas) {
		return fmt.Errorf("badger_storage.SetMulti: aggIds, deltas must be the same length")
	}
	if len(aggIds) == 0 {
		return nil
	}
	return tier.Badger.Update(func(txn *badger.Txn) error {
		for i, aggId := range aggIds {
			for _, bucket := range deltas[i] {
				k, err := badgerEncode(aggId, bucket)
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
		for i, aggId := range aggIds {
			tier.Logger.Info("Updating badger keys for aggregate",
				zap.Int("aggregate", int(aggId)),
				zap.Int("num_keys", len(deltas[i])),
			)
		}
		return nil
	})
}

func (b BadgerStorage) Set(ctx context.Context, tier tier.Tier, aggId ftypes.AggId, deltas []counter.Bucket) error {
	return b.SetMulti(ctx, tier, []ftypes.AggId{aggId}, [][]counter.Bucket{deltas})
}

var _ BucketStore = BadgerStorage{}

// defaultCodec key design is: codex | groupkey | window | width | index | aggregate_name
func badgerEncode(aggId ftypes.AggId, bucket counter.Bucket) ([]byte, error) {
	buf := make([]byte, 1+8+len(bucket.Key)+8+8+8+8+8)
	cur := 0
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
	if n, err := binary.PutUvarint(buf[cur:], uint64(aggId)); err != nil {
		return nil, err
	} else {
		cur += n
	}
	return buf[:cur], nil
}

func badgerDecode(buf []byte) (ftypes.AggId, counter.Bucket, error) {
	cur := 0

	codec, n, err := codex.Read(buf[cur:])
	if err != nil {
		return 0, counter.Bucket{}, err
	}
	if codec != defaultCodec {
		return 0, counter.Bucket{}, fmt.Errorf("badgerDecode: invalid codec: %v", codec)
	}
	cur += n

	key, n, err := binary.ReadString(buf[cur:])
	if err != nil {
		return 0, counter.Bucket{}, err
	} else {
		cur += n
	}
	window, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return 0, counter.Bucket{}, err
	} else {
		cur += n
	}
	width, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return 0, counter.Bucket{}, err
	} else {
		cur += n
	}
	index, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return 0, counter.Bucket{}, err
	} else {
		cur += n
	}
	aggId, n, err := binary.ReadUvarint(buf[cur:])
	if err != nil {
		return 0, counter.Bucket{}, err
	} else {
		cur += n
	}
	return ftypes.AggId(aggId), counter.Bucket{Key: key, Window: ftypes.Window(window), Width: width, Index: index}, nil
}
