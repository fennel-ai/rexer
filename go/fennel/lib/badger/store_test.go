package badger_test

import (
	"context"
	"testing"

	"fennel/lib/badger"
	"fennel/lib/kvstore"
	"fennel/test"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)

	txn := tier.Badger.NewTransaction(true)
	defer txn.Discard()

	store := badger.NewTransactionalStore(tier, 0, txn)

	ctx := context.Background()

	// Simple set and get operation.
	err = store.Set(ctx, []byte("key"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value"),
	})
	require.NoError(t, err)

	value, err := store.Get(ctx, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value.Raw)
	require.Equal(t, uint8(1), value.Codec)

	// Get value for key that has not been set.
	_, err = store.Get(ctx, []byte("key2"))
	require.Equal(t, err, kvstore.ErrKeyNotFound)
	// Now set the key in the db.
	err = store.Set(ctx, []byte("key2"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value2"),
	})
	require.NoError(t, err)
	// Get value for key that has been set.
	value, err = store.Get(ctx, []byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value.Raw)

	// Test errors on empty keys.
	_, err = store.Get(ctx, []byte(""))
	require.Equal(t, err, kvstore.ErrEmptyKey)

	err = store.Set(ctx, []byte(""), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value"),
	})
	require.Equal(t, err, kvstore.ErrEmptyKey)
}
