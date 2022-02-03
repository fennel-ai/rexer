package plane

import (
	"fennel/db"
	"fennel/kafka"
	"fennel/lib/cache"
	"fennel/lib/clock"
	"fennel/lib/ftypes"
	"fennel/redis"
)

/*
	Plane represents a full data plane of a particular customer. While each plane enjoys
	logical isolation, it may or may not get physical isolation of all the resources.
	Tier is a collection of planes that does get full isolation - it has its own database,
	own redis cluster, own cache cluster, own kafka cluster etc.
	Planes of a tier share the resources of the tier and only get logical isolation (e.g. each
	plane gets a set of tables prefixed by plane_id but all these tables will exist in the
	database that belongs to the tier.

	Note: for now, each tier has exactly one plane so the difference between them isn't very
	meaningful. But once we have a multi-tenant environment of freemium customers, a single
	tier will likely have multiple planes.
*/

type Plane struct {
	ID             ftypes.PlaneID
	TierID         ftypes.TierID
	CustID         ftypes.CustID
	DB             db.Connection
	Redis          redis.Client
	Cache          cache.Cache
	ActionProducer kafka.FProducer
	ActionConsumer kafka.FConsumer
	Clock          clock.Clock
}
