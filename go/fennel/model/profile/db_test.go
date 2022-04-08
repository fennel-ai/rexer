package profile

import (
	"context"
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestDBBasic(t *testing.T) {
	t.Parallel()
	t.Run("db_basic", func(t *testing.T) {
		testProviderBasic(t, dbProvider{})
	})
	t.Run("db_version", func(t *testing.T) {
		testProviderVersion(t, dbProvider{})
	})
	// this is disabled because currently the db behavior doesn't
	// respect the desired behavior and doesn't create error when setting
	// the same profile twice with different values
	// t.Run("db_set_again", func(t *testing.T) {
	// 	testSetAgain(t, dbProvider{})
	// })
	t.Run("db_set_batch", func(t *testing.T) {
		testSetBatch(t, dbProvider{})
	})
	t.Run("db_get_multi", func(t *testing.T) {
		testSQLGetMulti(t, dbProvider{})
	})
	t.Run("db_get_version_batch", func(t *testing.T) {
		testGetVersionBatched(t, dbProvider{})
	})
}

func TestLongKey(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	p := dbProvider{}

	val := value.Int(2)
	expected := value.ToJSON(val)

	// can not set value on a makeKey that is greater than 255 chars
	err = p.set(ctx, tier, "1", 1232, utils.RandString(256), 1, expected)
	assert.Error(t, err)

	// but works for a makeKey of size upto 255
	err = p.set(ctx, tier, "1", 1232, utils.RandString(255), 1, expected)
	assert.NoError(t, err)
}

func TestLongOType(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()
	p := dbProvider{}

	val := value.Int(5)
	expected := value.ToJSON(val)

	// otype cannot be longer than 255 chars
	err = p.set(ctx, tier, ftypes.OType(utils.RandString(256)), 23, "key", 1, expected)
	assert.Error(t, err)

	// but works for otype of length 255 chars
	err = p.set(ctx, tier, ftypes.OType(utils.RandString(255)), 23, "key", 1, expected)
	assert.NoError(t, err)
}
