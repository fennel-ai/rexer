package badger

import (
	"context"
	b64 "encoding/base64"

	"fennel/lib/kvstore"
	"fennel/tier"

	"github.com/dgraph-io/badger/v3"
	"go.uber.org/zap"
)

// BadgerTransactionalStore is a transactional KV store backed by BadgerDB.
// Here, "transactional" means that all operations are performed within a
// single BadgerDB transaction, and have "read-your-writes" semantics.
type BadgerTransactionalStore struct {
	tier tier.Tier
	txn  *badger.Txn
}

var _ kvstore.Reader = (*BadgerTransactionalStore)(nil)
var _ kvstore.Writer = (*BadgerTransactionalStore)(nil)

func NewTransactionalStore(tier tier.Tier, txn *badger.Txn) *BadgerTransactionalStore {
	return &BadgerTransactionalStore{
		tier: tier,
		txn:  txn,
	}
}

func (bs *BadgerTransactionalStore) Get(ctx context.Context, tablet kvstore.TabletType, key []byte) (*kvstore.SerializedValue, error) {
	if len(key) == 0 {
		return nil, kvstore.ErrEmptyKey
	}
	bs.tier.Logger.Debug("BadgerTransactionalStore.Get",
		zap.String("key", b64.StdEncoding.EncodeToString(key)),
	)
	key, err := makeKey(tablet, key)
	if err != nil {
		return nil, err
	}
	item, err := bs.txn.Get(key)
	switch err {
	case badger.ErrKeyNotFound:
		return nil, kvstore.ErrKeyNotFound
	case nil:
		var value kvstore.SerializedValue
		err = item.Value(func(v []byte) error {
			value.Codec = item.UserMeta()
			value.Raw = v
			return nil
		})
		if err != nil {
			return nil, err
		}
		return &value, nil
	default:
		return nil, err
	}
}

func (bs *BadgerTransactionalStore) Set(ctx context.Context, tablet kvstore.TabletType, key []byte, value kvstore.SerializedValue) error {
	if len(key) == 0 {
		return kvstore.ErrEmptyKey
	}
	bs.tier.Logger.Debug("BadgerTransactionalStore.Set",
		zap.String("key", b64.StdEncoding.EncodeToString(key)),
		zap.String("value", value.String()),
	)
	key, err := makeKey(tablet, key)
	if err != nil {
		return err
	}
	entry := badger.NewEntry(key, value.Raw).WithMeta(value.Codec)
	return bs.txn.SetEntry(entry)
}

func makeKey(tablet kvstore.TabletType, key []byte) ([]byte, error) {
	// TODO(abhay): Can we avoid an extra allocation and/or copy here?
	k := make([]byte, len(key)+1)
	n, err := tablet.Write(k)
	if err != nil {
		return nil, err
	}
	if n != 1 {
		panic("size of tablet type is not 1 byte")
	}
	copy(k[1:], key)
	return k, nil
}
