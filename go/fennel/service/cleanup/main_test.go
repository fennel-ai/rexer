package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/test"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestDeleteKeysForAggId(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()

	aggId := ftypes.AggId(20)

	keyPrefix, err := redisKeyPrefix(tier, aggId)
	assert.NoError(t, err)
	r := tier.Redis.Client().(*redis.ClusterClient)

	// store keys with prefix matching an aggId
	for i := 0; i < 100; i++ {
		// remove the trailing `*` wildcard
		assert.NoError(t, r.Set(ctx, fmt.Sprintf("%s%s", keyPrefix[:len(keyPrefix)-1], utils.RandString(10)), []byte(utils.RandString(4)), *new(time.Duration)).Err())
	}

	// insert different keys as well - these keys should not be deleted
	randomKeyPrefix := utils.RandString(3)
	expectedRandomKeys := make([]string, 0)
	for i := 0; i < 5; i++ {
		k := fmt.Sprintf("%s-%d", randomKeyPrefix, i)
		expectedRandomKeys = append(expectedRandomKeys, k)
		assert.NoError(t, r.Set(ctx, k, []byte(utils.RandString(2)), *new(time.Duration)).Err())
	}

	// request deleting keys with this prefix
	// setting a larger batch size for the test to finish quickly in integration mode
	assert.NoError(t, deleteKeys(tier, aggId, r, 100000 /*batchSize=*/))

	// check that keys matching this prefix do not exist
	actual, err := r.Keys(ctx, keyPrefix).Result()
	assert.NoError(t, err)
	assert.Empty(t, actual)

	// there should be keys matching
	randomKeyPattern := fmt.Sprintf("%s-*", randomKeyPrefix)
	a2, err := r.Keys(ctx, randomKeyPattern).Result()
	assert.NoError(t, err)
	assert.ElementsMatch(t, a2, expectedRandomKeys)
}
