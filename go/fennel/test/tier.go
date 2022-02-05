package test

import (
	"fennel/lib/clock"
	"math/rand"
	"time"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/redis"
	"fennel/tier"
)

// Tier returns a plane to be used in tests - this is based off a standard
// test plane and as many resources of the tier are mocked as possible
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
	kProducer, kConsumer, err := defaultProducerConsumer(tierID, action.ACTIONLOG_KAFKA_TOPIC)
	return tier.Tier{
		ID:             tierID,
		CustID:         ftypes.CustID(rand.Uint64()),
		DB:             db,
		Cache:          Cache,
		Redis:          redClient,
		ActionConsumer: kConsumer,
		ActionProducer: kProducer,
		Clock:          clock.Unix{},
	}, err
}

func Teardown(tier tier.Tier) error {
	return drop(tier.ID, logical_test_dbname, username, password, host)
}
