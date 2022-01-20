package test

import (
	"fennel/instance"
	"fennel/lib/action"
	"fennel/lib/utils"
	"fennel/redis"
	"fmt"
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
		DB:             db,
		Cache:          Cache,
		Name:           name,
		Redis:          redClient,
		Type:           instance.TEST,
		ActionConsumer: kConsumer,
		ActionProducer: kProducer,
	}, nil
}
