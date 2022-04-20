package badger

import (
	"context"
	"testing"

	"fennel/lib/kvstore"
	"fennel/test"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)

	txn := tier.Badger.NewTransaction(true)
	defer txn.Commit()

	store := NewTransactionalStore(tier, txn)

	ctx := context.Background()

	// Simple set and get operation.
	err = store.Set(ctx, kvstore.Profile, []byte("key"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value"),
	})
	require.NoError(t, err)

	value, err := store.Get(ctx, kvstore.Profile, []byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value.Raw)
	require.Equal(t, uint8(1), value.Codec)

	// Get value for key that has not been set.
	_, err = store.Get(ctx, kvstore.Profile, []byte("key2"))
	require.Equal(t, err, kvstore.ErrKeyNotFound)
	// Now set the key in the db.
	err = store.Set(ctx, kvstore.Profile, []byte("key2"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value2"),
	})
	require.NoError(t, err)
	// Get value for key that has been set.
	value, err = store.Get(ctx, kvstore.Profile, []byte("key2"))
	require.NoError(t, err)
	require.Equal(t, []byte("value2"), value.Raw)

	// Test errors on empty keys.
	_, err = store.Get(ctx, kvstore.Profile, []byte(""))
	require.Equal(t, err, kvstore.ErrEmptyKey)

	err = store.Set(ctx, kvstore.Profile, []byte(""), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value"),
	})
	require.Equal(t, err, kvstore.ErrEmptyKey)
}

func TestReadWrongTablet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)

	txn := tier.Badger.NewTransaction(true)
	defer txn.Commit()

	store := NewTransactionalStore(tier, txn)

	ctx := context.Background()

	// Simple set and get operation.
	err = store.Set(ctx, kvstore.Profile, []byte("key"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value"),
	})
	require.NoError(t, err)

	// Read from a different tablet should give an error.
	_, err = store.Get(ctx, kvstore.Aggregate, []byte("key"))
	require.Error(t, err)
}

func TestGetAll(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)

	txn := tier.Badger.NewTransaction(true)
	defer txn.Commit()

	store := NewTransactionalStore(tier, txn)

	ctx := context.Background()

	// Set 3 keys, 2 with the same prefix.
	err = store.Set(ctx, kvstore.Profile, []byte("key"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value"),
	})
	require.NoError(t, err)
	err = store.Set(ctx, kvstore.Profile, []byte("key2"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("value2"),
	})
	require.NoError(t, err)
	err = store.Set(ctx, kvstore.Profile, []byte("mykey"), kvstore.SerializedValue{
		Codec: 1,
		Raw:   []byte("myvalue"),
	})
	require.NoError(t, err)
	ks, vs, err := store.GetAll(ctx, kvstore.Profile, []byte("key"))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("key"), []byte("key2")}, ks)
	require.Equal(t, len(ks), len(vs))

	ks, vs, err = store.GetAll(ctx, kvstore.Profile, []byte("my"))
	require.NoError(t, err)
	require.ElementsMatch(t, [][]byte{[]byte("mykey")}, ks)
	require.Equal(t, len(ks), len(vs))
}
