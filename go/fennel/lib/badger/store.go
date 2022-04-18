package badger

import (
	"context"

	"fennel/lib/kvstore"
	"fennel/tier"

	"github.com/dgraph-io/badger/v3"
)

// BadgerTransactionalStore is a transactional KV store backed by BadgerDB.
// Here, "transactional" means that all operations are performed within a
// single BadgerDB transaction, and have "read-your-writes" semantics.
type BadgerTransactionalStore struct {
	tier   tier.Tier
	tablet uint8
	txn    *badger.Txn
}

var _ kvstore.Reader = (*BadgerTransactionalStore)(nil)
var _ kvstore.Writer = (*BadgerTransactionalStore)(nil)

func NewTransactionalStore(tier tier.Tier, tablet uint8, txn *badger.Txn) *BadgerTransactionalStore {
	return &BadgerTransactionalStore{
		tier:   tier,
		tablet: tablet,
		txn:    txn,
	}
}

func (bs *BadgerTransactionalStore) Get(ctx context.Context, key []byte) (*kvstore.SerializedValue, error) {
	if len(key) == 0 {
		return nil, kvstore.ErrEmptyKey
	}
	key = makeKey(bs.tablet, key)
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

func (bs *BadgerTransactionalStore) Set(ctx context.Context, key []byte, value kvstore.SerializedValue) error {
	if len(key) == 0 {
		return kvstore.ErrEmptyKey
	}
	key = makeKey(bs.tablet, key)
	entry := badger.NewEntry(key, value.Raw).WithMeta(value.Codec)
	return bs.txn.SetEntry(entry)
}

func makeKey(tablet uint8, key []byte) []byte {
	// TODO(abhay): Can we avoid an extra allocation and copy here?
	k := make([]byte, len(key)+1)
	k[0] = tablet
	copy(k[1:], key)
	return k
}
