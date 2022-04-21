package tailer

import (
	"context"
	"testing"
	"time"

	"fennel/controller/profile"
	"fennel/lib/badger"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	profilekv "fennel/model/profile/kv"
	"fennel/test"

	db "github.com/dgraph-io/badger/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProfileSet(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	// Start a consumer to read the profile log and write to kv store.
	closeCh := make(chan struct{})
	writeProfilesToLocalKvStore(tier, closeCh)

	// Write a profile to kafka.
	p := profilelib.NewProfileItem("User", "1232", "summary", value.Int(1), 100)
	err = profile.Set(context.Background(), tier, p)
	require.NoError(t, err)

	// Wait for the profile to be written to kv store.
	var readProfile profilelib.ProfileItem
	for {
		time.Sleep(time.Second * 1)
		pi, err := profile.Get(context.Background(), tier, p.GetProfileKey())
		require.NoError(t, err)
		if pi.UpdateTime != 0 {
			readProfile = pi
			break
		}
	}
	require.Equal(t, p, readProfile)

	// Close the channel to stop the consumer.
	close(closeCh)

	// Set the profile to a different value (but older update time) directly in the store.
	pnew := p
	pnew.Value = value.NewList(value.Int(2), value.Int(3))
	err = tier.Badger.Update(func(txn *db.Txn) error {
		writer := badger.NewTransactionalStore(tier, txn)
		return profilekv.Set(context.Background(), []profilelib.ProfileItem{pnew}, writer)
	})
	require.NoError(t, err)

	// Also log a new profile to kafka.
	p2 := profilelib.NewProfileItem("User", "456", "summary2", value.Int(42), 10)
	err = profile.Set(context.Background(), tier, p2)
	require.NoError(t, err)

	// Now start a new consumer to read the new profile from kafka and write to kv store.
	closeCh = make(chan struct{})
	writeProfilesToLocalKvStore(tier, closeCh)

	// Wait till p2 is writte to kv store.
	for {
		time.Sleep(time.Second * 1)
		pi, err := profile.Get(context.Background(), tier, p2.GetProfileKey())
		require.NoError(t, err)
		if pi.UpdateTime != 0 {
			readProfile = pi
			break
		}
	}
	require.Equal(t, p2, readProfile)

	// Close the channel to stop the consumer.
	close(closeCh)

	// Now, read the first profile from the kv store. It should be same as p2
	// and not p, which validates that the consumer did not process the earlier
	// message in kafka.
	got, err := profile.Get(context.Background(), tier, p.GetProfileKey())
	require.NoError(t, err)
	require.Equal(t, p.GetProfileKey(), pnew.GetProfileKey())
	require.Equal(t, pnew, got)
	require.NotEqual(t, p, got)
}
