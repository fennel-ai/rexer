//go:build integration

package test

import (
	fkafka "fennel/kafka"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/redis"
	"fennel/tier"
	"fmt"
	"math/rand"
	"time"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is given, all resources are real
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

	// set up kafka for integration
	if err = setupKafkaTopics(tierID, fkafka.ALL_TOPICS); err != nil {
		return tier.Tier{}, err
	}
	producers, consumers, err := tier.CreateKafka(tierID, test_kafka_servers, kafka_username, kafka_password)
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

	if err := teardownKafkaTopics(tier.ID, fkafka.ALL_TOPICS); err != nil {
		panic(fmt.Sprintf("unable to teardown kafka topics: %v", err))
		return err
	}
	return nil
}
