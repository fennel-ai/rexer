package counter

import (
	"fennel/lib/ftypes"
	value "fennel/lib/value"
)

const (
	AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME = "aggr_offline_transform"
)

type Bucket struct {
	Key    string
	Window ftypes.Window
	Width  uint64
	Index  uint64
	Value  value.Value
}
