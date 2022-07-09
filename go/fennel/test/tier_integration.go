//go:build integration

package test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/milvus"
	"fennel/resource"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is given, all resources are real
func Tier(t *testing.T) tier.Tier {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	var flags tier.TierArgs
	// Parse flags / environment variables.
	arg.Parse(&flags)
	flags.Dev = true
	flags.TierID = tierID
	// If PlaneID has not been set, genarate a random one.
	if flags.PlaneID == 0 {
		flags.PlaneID = ftypes.RealmID(rand.Uint32())
	}
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
	if err := setupDB(flags.TierID, flags.MysqlDB, flags.MysqlUsername, flags.MysqlPassword, flags.MysqlHost); err != nil {
		return err
	}
	return SetupKafkaTopics(resource.NewTierScope(flags.TierID), flags.KafkaServer, flags.KafkaUsername, flags.KafkaPassword)
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

	if err := teardownKafkaTopics(tr.ID, flags.KafkaServer, flags.KafkaUsername, flags.KafkaPassword, fkafka.ALL_TOPICS); err != nil {
		panic(fmt.Sprintf("unable to teardown kafka topics: %v", err))
	}
	var err error
	tr.MilvusClient.ForEach(func(client milvus.Client) { err = client.Close() })
	return err
}

func teardownKafkaTopics(tierID ftypes.RealmID, host, username, password string, topics []fkafka.TopicConf) error {
	scope := resource.NewTierScope(tierID)
	names := make([]string, len(topics))
	for i, topic := range topics {
		names[i] = scope.PrefixedName(topic.Topic)
	}
	// Create admin client.
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password))
	if err != nil {
		return fmt.Errorf("failed to create admin client: %v", err)
	}
	defer c.Close()

	// delete any existing topics of these names
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err = c.DeleteTopics(ctx, names)
	return err
}
