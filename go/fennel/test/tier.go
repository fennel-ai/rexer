package test

import (
	"fennel/kafka"
	"fennel/lib/clock"
	"flag"
	"fmt"
	"math/rand"
	"time"

	"fennel/lib/ftypes"
	"fennel/redis"
	"fennel/tier"
)

var integration = flag.Bool("integration", false, "flag to indicate whether to run integration tests")

// Tier returns a tier to be used in tests based off a standard  test plane
// if 'integration' flag is set, real resources are used, else resources are mocked whenever possible
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
	producers, consumers, err := createKafka(tierID, *integration)
	if err != nil {
		return tier.Tier{}, err
	}
	return tier.Tier{
		ID:        tierID,
		CustID:    ftypes.CustID(rand.Uint64()),
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

	if *integration {
		if err := teardownKafkaTopics(tier.ID, kafka.ALL_TOPICS); err != nil {
			panic(fmt.Sprintf("unable to teardown kafka topics: %v", err))
			return err
		}
	}
	return nil
}
