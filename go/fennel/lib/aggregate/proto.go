package aggregate

import (
	"fennel/lib/ftypes"
)

func FromProtoOptions(popt *AggOptions) Options {
	return Options{
		AggType:         ftypes.AggType(popt.AggType),
		Durations:       popt.Durations,
		Window:          popt.Window,
		Limit:           popt.Limit,
		Normalize:       popt.Normalize,
		CronSchedule:    popt.CronSchedule,
		Dim:             popt.Dim,
		HyperParameters: popt.HyperParameters,
	}
}

func ToProtoOptions(opt Options) *AggOptions {
	return &AggOptions{
		AggType:         string(opt.AggType),
		Durations:       opt.Durations,
		Window:          opt.Window,
		Limit:           opt.Limit,
		Normalize:       opt.Normalize,
		CronSchedule:    opt.CronSchedule,
		Dim:             opt.Dim,
		HyperParameters: opt.HyperParameters,
	}
}
