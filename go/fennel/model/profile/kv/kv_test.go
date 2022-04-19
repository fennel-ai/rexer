package kv

import (
	"context"
	"testing"

	"fennel/lib/badger"
	"fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/require"
)

func TestSetGet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, 0, txn)

	p := profile.NewProfileItem("mytype", "13", "mykey", value.Int(42), 25)
	err = Set(ctx, []profile.ProfileItem{p}, store)
	require.NoError(t, err)
	got, err := Get(ctx, []profile.ProfileItemKey{p.GetProfileKey()}, store)
	require.NoError(t, err)
	require.Equal(t, p, got[0])
}

func TestGetWithoutSet(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, 0, txn)

	p := profile.NewProfileItem("mytype", "13", "mykey", value.Int(42), 25)
	// Getting a key that has not been set should return a nil value.
	got, err := Get(ctx, []profile.ProfileItemKey{p.GetProfileKey()}, store)
	require.NoError(t, err)
	require.Equal(t, 1, len(got))
	require.Equal(t, value.Nil, got[0].Value)
	// Now set the profile.
	err = Set(ctx, []profile.ProfileItem{p}, store)
	require.NoError(t, err)
	got, err = Get(ctx, []profile.ProfileItemKey{p.GetProfileKey()}, store)
	require.NoError(t, err)
	require.Equal(t, 1, len(got))
	require.Equal(t, p, got[0])
}

func TestSetOlderValue(t *testing.T) {
	tier, err := test.Tier()
	require.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	txn := tier.Badger.NewTransaction(true)
	store := badger.NewTransactionalStore(tier, 0, txn)

	p := profile.NewProfileItem("mytype", "13", "mykey", value.Int(42), 25)
	err = Set(ctx, []profile.ProfileItem{p}, store)
	require.NoError(t, err)

	got, err := Get(ctx, []profile.ProfileItemKey{p.GetProfileKey()}, store)
	require.NoError(t, err)
	require.Equal(t, p, got[0])

	// Try setting p's value with an older update time.
	pold := p
	pold.UpdateTime = p.UpdateTime - 1
	err = Set(ctx, []profile.ProfileItem{pold}, store)
	require.NoError(t, err)
	// Read should still return p.
	got, err = Get(ctx, []profile.ProfileItemKey{p.GetProfileKey()}, store)
	require.NoError(t, err)
	require.Equal(t, p, got[0])

	// Setting newer value should update the value in store.
	pnew := p
	pnew.UpdateTime = p.UpdateTime + 1
	err = Set(ctx, []profile.ProfileItem{pnew}, store)
	require.NoError(t, err)
	// Read should still return p.
	got, err = Get(ctx, []profile.ProfileItemKey{p.GetProfileKey()}, store)
	require.NoError(t, err)
	require.Equal(t, pnew, got[0])
}
