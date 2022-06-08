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
	Width  uint32
	// We set Index as `uint32` which has range [0, 4.2e^9]. With `Width` = 1, index will be in range till year ~2106
	// assuming timestamp is in seconds since epoch, which in our case is.
	Index uint32
}

type BucketList struct {
	Key        string
	Window     ftypes.Window
	Width      uint32
	StartIndex uint32
	EndIndex   uint32
}
