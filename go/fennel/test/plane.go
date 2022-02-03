package test

import (
	"fennel/lib/clock"
	"math/rand"
	"time"

	"fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/plane"
	"fennel/redis"
)

// MockPlane returns a plane to be used in tests - as many resources of the plane
// are mocked as possible
func MockPlane() (plane.Plane, error) {
	rand.Seed(time.Now().UnixNano())
	planeID := ftypes.PlaneID(rand.Uint32())
	db, err := defaultDB(planeID)
	if err != nil {
		return plane.Plane{}, err
	}
	resource, err := DefaultRedis()
	if err != nil {
		return plane.Plane{}, err
	}
	redClient := resource.(redis.Client)

	Cache := redis.NewCache(redClient)
	kProducer, kConsumer, err := DefaultProducerConsumer(action.ACTIONLOG_KAFKA_TOPIC)
	return plane.Plane{
		ID:             planeID,
		TierID:         ftypes.TierID(rand.Uint32()),
		CustID:         ftypes.CustID(rand.Uint64()),
		DB:             db,
		Cache:          Cache,
		Redis:          redClient,
		ActionConsumer: kConsumer,
		ActionProducer: kProducer,
		Clock:          clock.Unix{},
	}, err
}
