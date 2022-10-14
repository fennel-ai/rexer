//go:build integration

package test

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	testnitrous "fennel/featurestore/test/nitrous"
	"fennel/featurestore/tier"
	"fennel/lib/ftypes"
	nitrous "fennel/nitrous/featurestore/test"
	"github.com/alexflint/go-arg"
	"github.com/stretchr/testify/assert"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is given, all resources are real
func Tier(t *testing.T) tier.Tier {
	rand.Seed(time.Now().UnixNano())
	// Setup plane-level nitrous.
	tn := nitrous.NewTestNitrous(t)
	_, _ = testnitrous.StartNitrousServer(t, tn.Nitrous)
	// Parse flags / environment variables.
	var flags tier.TierArgs
	arg.Parse(&flags)
	flags.Dev = true
	flags.PlaneID = tn.PlaneID
	flags.TierID = ftypes.RealmID(rand.Uint32())
	err := flags.Valid()
	assert.NoError(t, err)
	// do all Setup that needs to be done to setup a valid tier
	err = SetupTier(flags)
	assert.NoError(t, err)
	// finally, instantiate and return the tier
	tier, err := tier.CreateFromArgs(&flags)
	assert.NoError(t, err)
	return tier
}

func SetupTier(flags tier.TierArgs) error {
	return setupDB(flags.TierID, flags.MysqlDB, flags.MysqlUsername, flags.MysqlPassword, flags.MysqlHost)
}

func Teardown(tr tier.Tier) error {
	var flags tier.TierArgs
	// Parse flags / environment variables.
	arg.Parse(&flags)
	flags.TierID = tr.ID
	if err := flags.Valid(); err != nil {
		return err
	}

	if err := drop(tr.ID, flags.MysqlDB, flags.MysqlUsername, flags.MysqlPassword, flags.MysqlHost); err != nil {
		panic(fmt.Sprintf("error in db teardown: %v\n", err))
	}
	return nil
}
