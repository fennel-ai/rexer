//go:build integration

package test

import (
	"fmt"
	"math/rand"
	"time"

	fkafka "fennel/kafka"
	"fennel/lib/ftypes"
	"fennel/tier"

	"github.com/alexflint/go-arg"
)

// Tier returns a tier to be used in tests based off a standard test plane
// since this is only compiled when 'integration' build tag is given, all resources are real
func Tier() (tier.Tier, error) {
	rand.Seed(time.Now().UnixNano())
	tierID := ftypes.RealmID(rand.Uint32())
	var flags tier.TierArgs
	// Parse flags / environment variables.
	arg.Parse(&flags)
	flags.Dev = true
	flags.TierID = tierID
	if err := flags.Valid(); err != nil {
		return tier.Tier{}, err
	}
	// do all Setup that needs to be done to setup a valid tier
	if err := Setup(flags); err != nil {
		return tier.Tier{}, err
	}

	// finally, instantiate and return the tier
	return tier.CreateFromArgs(&flags)
}

func Setup(flags tier.TierArgs) error {
	if err := setupDB(flags.TierID, flags.MysqlDB, flags.MysqlUsername, flags.MysqlPassword, flags.MysqlHost); err != nil {
		return err
	}
	return setupKafkaTopics(flags.TierID, flags.KafkaServer, flags.KafkaUsername, flags.KafkaPassword, fkafka.ALL_TOPICS)
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

	if err := teardownBadger(tr.Badger); err != nil {
		panic(fmt.Sprintf("unable to teardown badger: %v", err))
	}
	return nil
}
