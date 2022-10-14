//go:build !integration

package test

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"fennel/featurestore/test/kafka"
	"fennel/featurestore/test/nitrous"
	"fennel/featurestore/tier"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	unleashlib "fennel/lib/unleash"
	"fennel/pcache"
	"fennel/s3"

	"github.com/Unleash/unleash-client-go/v3"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is not given, most resources are mocked
func Tier(t *testing.T) tier.Tier {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())

	db, err := defaultDB(tierID, "testdb" /*logicalname*/, os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_SERVER_ADDRESS"))
	assert.NoError(t, err)

	producers, consumerCreator, err := kafka.CreateMockKafka(tierID)
	assert.NoError(t, err)

	PCache, err := pcache.NewPCache(1<<31, 1<<6)
	assert.NoError(t, err)

	s3Client := s3.NewClient(s3.S3Args{Region: "ap-south-1"})

	logger, err := zap.NewDevelopment()
	assert.NoError(t, err)
	logger = logger.With(zap.Uint32("tier_id", uint32(tierID)))

	faker := unleashlib.NewFakeUnleash()
	err = unleash.Initialize(unleash.WithListener(&unleash.DebugListener{}),
		unleash.WithAppName("local-tier"),
		unleash.WithUrl(faker.Url()))
	assert.NoError(t, err)

	_, nitrousClient := nitrous.NewLocalClient(t, tierID)

	return tier.Tier{
		ID:               tierID,
		DB:               db,
		PCache:           PCache,
		NitrousClient:    mo.Some(nitrousClient),
		Producers:        producers,
		Clock:            clock.Unix{},
		NewKafkaConsumer: tier.KafkaConsumerCreator(consumerCreator),
		S3Client:         s3Client,
		Logger:           logger,
		RequestLimit:     -1,
	}
}

func TierWithRequestLimit(t *testing.T, requestLimit int64) tier.Tier {
	tier := Tier(t)
	tier.RequestLimit = requestLimit
	return tier
}

func Teardown(tier tier.Tier) {
	if err := drop(tier.ID, "testdb" /*logicalname*/, os.Getenv("MYSQL_USERNAME"), os.Getenv("MYSQL_PASSWORD"), os.Getenv("MYSQL_SERVER_ADDRESS")); err != nil {
		panic(fmt.Sprintf("error in db teardown: %v\n", err))
	}
}
