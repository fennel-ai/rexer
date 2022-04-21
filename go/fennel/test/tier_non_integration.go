//go:build !integration

package test

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/modelstore"
	"fennel/pcache"
	"fennel/redis"
	"fennel/s3"
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

	PCache, err := pcache.NewPCache(1<<31, 1<<6)
	if err != nil {
		return tier.Tier{}, err
	}

	// TODO - decide what region to use for test tier
	s3Client := s3.NewClient(s3.S3Args{Region: "ap-south-1"}, tierID)

	modelStore := modelstore.NewModelStore(modelstore.ModelStoreArgs{
		ModelStoreS3Bucket:     os.Getenv("MODEL_STORE_S3_BUCKET"),
		ModelStoreEndpointName: os.Getenv("MODEL_STORE_ENDPOINT"),
	}, tierID)

	logger, err := zap.NewDevelopment()
	if err != nil {
		return tier.Tier{}, fmt.Errorf("failed to construct logger: %v", err)
	}
	logger = logger.With(zap.Uint32("tier_id", uint32(tierID)))
	badger, err := defaultBadger(tierID)
	if err != nil {
		return tier.Tier{}, err
	}
	return tier.Tier{
		ID:               tierID,
		DB:               db,
		Cache:            Cache,
		PCache:           PCache,
		Redis:            redClient,
		Producers:        producers,
		Clock:            clock.Unix{},
		NewKafkaConsumer: consumerCreator,
		S3Client:         s3Client,
		ModelStore:       modelStore,
		Logger:           logger,
		Badger:           badger,
	}, nil
}

func Teardown(tier tier.Tier) error {
	if err := drop(tier.ID, "testdb" /*logicalname*/, os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_SERVER_ADDRESS")); err != nil {
		panic(fmt.Sprintf("error in db teardown: %v\n", err))
	}
	if err := teardownBadger(tier.Badger); err != nil {
		panic(fmt.Sprintf("error in badger teardown: %v\n", err))
	}
	return nil
}
