package counter

import (
	"fmt"

	"fennel/lib/aggregate"
	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/samber/mo"
)

type MergeReduce interface {
	Options() aggregate.Options
	Transform(v value.Value) (value.Value, error)
	Merge(a, b value.Value) (value.Value, error)
	Reduce(values []value.Value) (value.Value, error)
	Zero() value.Value
}

func Start(mr MergeReduce, end ftypes.Timestamp, duration mo.Option[uint32]) (ftypes.Timestamp, error) {
	switch mr.Options().AggType {
	case aggregate.TIMESERIES_SUM:
		var d uint32
		opts := mr.Options()
		switch opts.Window {
		case ftypes.Window_HOUR:
			d = uint32(1+opts.Limit) * 3600
		case ftypes.Window_DAY:
			d = uint32(1+opts.Limit) * 3600 * 24
		}
		return start(end, d), nil
	default:
		if duration.IsAbsent() {
			return 0, fmt.Errorf("duration is required for aggregate to ftype %v", mr.Options().AggType)
		}
		return start(end, duration.MustGet()), nil
	}
}

func ToMergeReduce(aggId ftypes.AggId, opts aggregate.Options) (MergeReduce, error) {
	var mr MergeReduce
	switch opts.AggType {
	case aggregate.TIMESERIES_SUM:
		mr = NewTimeseriesSum(opts)
	case aggregate.SUM:
		mr = NewSum(opts)
	case aggregate.AVERAGE:
		mr = NewAverage(opts)
	case aggregate.LIST:
		mr = NewList(opts)
	case aggregate.MIN:
		mr = NewMin(opts)
	case aggregate.MAX:
		mr = NewMax(opts)
	case aggregate.STDDEV:
		mr = NewStdDev(opts)
	case aggregate.RATE:
		mr = NewRate(aggId, opts)
	case aggregate.TOPK:
		mr = NewTopK(opts)
	default:
		return nil, fmt.Errorf("invalid aggregate type: %v", opts.AggType)
	}
	return mr, nil
}
