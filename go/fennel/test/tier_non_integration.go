//go:build !integration

package test

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/redis"
	"fennel/tier"

	"go.uber.org/zap"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is not given, most resources are mocked
func Tier() (tier.Tier, error) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	db, err := defaultDB(tierID, "testdb" /*logicalname*/, os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_SERVER_ADDRESS"))
	if err != nil {
		return tier.Tier{}, err
	}
	redClient, err := mockRedis(tierID)
	if err != nil {
		return tier.Tier{}, err
	}

	Cache := redis.NewCache(redClient)
	producers, consumerCreator, err := createMockKafka(tierID)
	if err != nil {
		return tier.Tier{}, err
	}
	logger, err := zap.NewDevelopment()
	if err != nil {
		return tier.Tier{}, fmt.Errorf("failed to construct logger: %v", err)
	}
	logger = logger.With(zap.Uint32("tier_id", uint32(tierID)))
	return tier.Tier{
		ID:               tierID,
		DB:               db,
		Cache:            Cache,
		Redis:            redClient,
		Producers:        producers,
		Clock:            clock.Unix{},
		NewKafkaConsumer: consumerCreator,
		Logger:           logger,
	}, err
}

func Teardown(tier tier.Tier) error {
	if err := drop(tier.ID, "testdb" /*logicalname*/, os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_SERVER_ADDRESS")); err != nil {
		panic(fmt.Sprintf("error in db teardown: %v\n", err))
	}
	return nil
}
