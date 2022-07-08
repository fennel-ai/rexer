package store

import (
	"context"
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/value"

	"github.com/samber/mo"
)

type AggregateStore interface {
	Get(ctx context.Context, kwargs []value.Dict, keys []string) ([]value.Value, error)
}

func getRequestDuration(options aggregate.Options, kwargs value.Dict) (mo.Option[uint32], error) {
	if options.AggType == aggregate.TIMESERIES_SUM {
		return mo.None[uint32](), nil
	}
	d, err := extractDuration(kwargs, options.Durations)
	if err != nil {
		return mo.None[uint32](), err
	}
	return mo.Some(d), nil
}

func extractDuration(kwargs value.Dict, durations []uint32) (uint32, error) {
	v, ok := kwargs.Get("duration")
	if !ok {
		return 0, fmt.Errorf("error: no duration specified")
	}
	duration, ok := v.(value.Int)
	if !ok {
		return 0, fmt.Errorf("error: expected kwarg 'duration' to be an int but found: '%v'", v)
	}
	// check duration is positive so it can be typecast to uint32 safely
	if duration < 0 {
		return 0, fmt.Errorf("error: specified duration (%d) < 0", duration)
	}
	for _, d := range durations {
		if uint32(duration) == d {
			return d, nil
		}
	}
	return 0, fmt.Errorf("error: specified duration not found in aggregate")
}
