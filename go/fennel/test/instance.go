package test

import (
	"fennel/lib/clock"
	"fmt"
	"math/rand"

	"fennel/instance"
	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/redis"
)

func DefaultInstance() (instance.Instance, error) {
	db, err := DefaultDB()
	if err != nil {
		return instance.Instance{}, err
	}
	resource, err := DefaultRedis()
	if err != nil {
		return instance.Instance{}, err
	}
	redClient := resource.(redis.Client)

	Cache := redis.NewCache(redClient)
	name := fmt.Sprintf("test_%s", utils.RandString(6))
	kProducer, kConsumer, err := DefaultProducerConsumer(action.ACTIONLOG_KAFKA_TOPIC)
	return instance.Instance{
		CustID:         ftypes.CustID(rand.Uint64()),
		DB:             db,
		Cache:          Cache,
		Name:           name,
		Redis:          redClient,
		Type:           instance.TEST,
		ActionConsumer: kConsumer,
		ActionProducer: kProducer,
		Clock:          clock.Unix{},
	}, err
}
