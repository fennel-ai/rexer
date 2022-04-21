package counter

import (
	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	value "fennel/lib/value"
)

const (
	AGGREGATE_DELTA_TOPIC_NAME             = "aggr_delta"
	AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME = "aggr_offline_transform"
)

type AggregateDelta struct {
	AggId   ftypes.AggId
	Options aggregate.Options
	Buckets []Bucket
}

type Bucket struct {
	Key    string
	Window ftypes.Window
	Width  uint64
	Index  uint64
	Value  value.Value
}
