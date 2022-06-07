package counter

import (
	"fennel/lib/ftypes"
)

const (
	AGGREGATE_OFFLINE_TRANSFORM_TOPIC_NAME = "aggr_offline_transform"
)

type Bucket struct {
	Key    string
	Window ftypes.Window
	Width  uint64
	Index  uint64
}
