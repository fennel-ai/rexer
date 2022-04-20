package kv

import (
	"context"
	"testing"

	"fennel/lib/badger"
	"fennel/lib/counter"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/require"
)

func TestSetGetLenMustMatch(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, txn)

	require.Error(t, Set(ctx, tier, []ftypes.AggId{12}, [][]counter.Bucket{}, store))
	_, err = Get(ctx, tier, []ftypes.AggId{12}, [][]counter.Bucket{}, []value.Value{value.Nil}, store)
	require.Error(t, err)

	// default has diff length
	_, err = Get(ctx, tier, []ftypes.AggId{12}, [][]counter.Bucket{
		{
			counter.Bucket{
				Key:    "12",
				Window: 11,
				Width:  6,
				Index:  123,
				Value:  value.String("foo"),
			},
		}}, []value.Value{}, store)
	require.Error(t, err)
}

func TestSetGet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, txn)

	deltas := make([][]counter.Bucket, 2)
	deltas[0] = []counter.Bucket{
		{
			Key:    "12",
			Window: 11,
			Width:  6,
			Index:  123,
			Value:  value.String("foo"),
		},
		{
			Key:    "11",
			Window: 10,
			Width:  12,
			Index:  1234,
			Value:  value.String("bar"),
		},
	}
	deltas[1] = []counter.Bucket{
		{
			Key:    "21",
			Window: 21,
			Width:  18,
			Index:  21,
			Value:  value.String("foo2"),
		},
	}
	err = Set(ctx, tier, []ftypes.AggId{12, 23}, deltas, store)
	require.NoError(t, err)

	// Read one AggId
	{
		got, err := Get(ctx, tier, []ftypes.AggId{12}, [][]counter.Bucket{deltas[0]}, []value.Value{value.Nil}, store)
		require.NoError(t, err)
		require.Equal(t, []value.Value{value.String("foo"), value.String("bar")}, got[0])
	}
	// Read both
	{
		got, err := Get(ctx, tier, []ftypes.AggId{12, 23}, deltas, []value.Value{value.Nil, value.Nil}, store)
		require.NoError(t, err)
		require.Equal(t, [][]value.Value{{value.String("foo"), value.String("bar")}, {value.String("foo2")}}, got)
	}
}

func TestGetWithoutSetReturnsDefault(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, txn)

	// Getting a key that has not been set should return a default value.
	got, err := Get(ctx, tier, []ftypes.AggId{12}, [][]counter.Bucket{
		{
			counter.Bucket{
				Key: "12", Window: 11,
				Width: 6,
				Index: 123,
				Value: value.String("foo"),
			},
		}}, []value.Value{value.String("default")}, store)
	require.NoError(t, err)
	require.Equal(t, 1, len(got))
	require.Equal(t, []value.Value{value.String("default")}, got[0])
}
