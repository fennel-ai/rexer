//go:build !integration

package test

import (
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/redis"
	"fennel/tier"
	"fmt"
	"math/rand"
	"time"
)

// Tier returns a tier to be used in tests based off a standard  test plane
// since this is only compiled when 'integration' build tag is not given, most resources are mocked
func Tier() (tier.Tier, error) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.TierID(rand.Uint32())
	db, err := defaultDB(tierID)
	if err != nil {
		return tier.Tier{}, err
	}
	resource, err := DefaultRedis()
	if err != nil {
		return tier.Tier{}, err
	}
	redClient := resource.(redis.Client)

	Cache := redis.NewCache(redClient)
	producers, consumers, err := createMockKafka(tierID)
	if err != nil {
		return tier.Tier{}, err
	}
	return tier.Tier{
		ID:        tierID,
		DB:        db,
		Cache:     Cache,
		Redis:     redClient,
		Producers: producers,
		Consumers: consumers,
		Clock:     clock.Unix{},
	}, err
}

func Teardown(tier tier.Tier) error {
	if err := drop(tier.ID, logical_test_dbname, username, password, host); err != nil {
		panic(fmt.Sprintf("error in db teardown: %v\n", err))
		return err
	}
	return nil
}
