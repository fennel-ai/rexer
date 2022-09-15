//go:build integration

package test

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/milvus"
	nitrous "fennel/nitrous/test"
	"fennel/resource"
	testkafka "fennel/test/kafka"
	testnitrous "fennel/test/nitrous"
	"fennel/tier"

	"github.com/alexflint/go-arg"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is given, all resources are real
func Tier(t *testing.T) tier.Tier {
	rand.Seed(time.Now().UnixNano())
	// Setup plane-level nitrous.
	tn := nitrous.NewTestNitrous(t)
	_, _ = testnitrous.StartNitrousServer(t, tn.Nitrous)
	// Parse flags / environment variables.
	var flags tier.TierArgs
	arg.Parse(&flags)
	flags.Dev = true
	flags.PlaneID = tn.PlaneID
	flags.TierID = ftypes.RealmID(rand.Uint32())
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

	// integration tests should write and read from the MSK cluster - this is because there are no mirrors configured
	// in this scenario
	return testkafka.SetupKafkaTopics(resource.NewTierScope(flags.TierID), flags.MskKafkaServer, flags.MskKafkaUsername, flags.MskKafkaPassword, fkafka.SaslScramSha512Mechanism, append(fkafka.ALL_MSK_TOPICS, fkafka.ALL_CONFLUENT_TOPICS...))
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

	// tier down all the topics from MSK
	if err := teardownKafkaTopics(tr.ID, flags.MskKafkaServer, flags.MskKafkaUsername, flags.MskKafkaPassword, fkafka.SaslScramSha512Mechanism); err != nil {
		panic(fmt.Sprintf("unable to teardown msk kafka topics: %v", err))
	}
	var err error
	tr.MilvusClient.ForEach(func(client milvus.Client) { err = client.Close() })
	return err
}

func teardownKafkaTopics(tierID ftypes.RealmID, host, username, password, saslMechanism string) error {
	scope := resource.NewTierScope(tierID)
	names := make([]string, 0)
	for _, topic := range append(fkafka.ALL_CONFLUENT_TOPICS, fkafka.ALL_MSK_TOPICS...) {
		if reflect.TypeOf(scope) == reflect.TypeOf(topic.Scope) {
			names = append(names, scope.PrefixedName(topic.Topic))
		}
	}
	// Create admin client.
	c, err := kafka.NewAdminClient(fkafka.ConfigMap(host, username, password, saslMechanism))
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
