package aggregate

import (
	"strings"

	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

func FromProtoGetAggValueRequest(pr *ProtoGetAggValueRequest) (GetAggValueRequest, error) {
	key, err := value.FromProtoValue(pr.GetKey())
	if err != nil {
		return GetAggValueRequest{}, err
	}
	return GetAggValueRequest{
		AggName: ftypes.AggName(pr.GetAggName()),
		Key:     key,
	}, nil
}

func ToProtoGetAggValueRequest(gavr GetAggValueRequest) (ProtoGetAggValueRequest, error) {
	pkey, err := value.ToProtoValue(gavr.Key)
	if err != nil {
		return ProtoGetAggValueRequest{}, nil
	}
	return ProtoGetAggValueRequest{
		AggName: string(gavr.AggName),
		Key:     &pkey,
	}, nil
}

func ToProtoAggregate(agg Aggregate) (ProtoAggregate, error) {
	pquery, err := ast.ToProtoAst(agg.Query)
	if err != nil {
		return ProtoAggregate{}, err
	}
	return ProtoAggregate{
		AggName:   string(agg.Name),
		Query:     &pquery,
		Timestamp: uint64(agg.Timestamp),
		Options:   ToProtoOptions(agg.Options),
	}, nil
}

func FromProtoAggregate(pagg *ProtoAggregate) (Aggregate, error) {
	query, err := ast.FromProtoAst(pagg.Query)
	if err != nil {
		return Aggregate{}, err
	}
	agg := Aggregate{
		Name:      ftypes.AggName(strings.ToLower(pagg.AggName)),
		Query:     query,
		Timestamp: ftypes.Timestamp(pagg.Timestamp),
		Options:   FromProtoOptions(pagg.Options),
	}
	agg.Options.AggType = ftypes.AggType(strings.ToLower(string(agg.Options.AggType)))
	return agg, nil
}

func FromProtoOptions(popt *AggOptions) Options {
	return Options{
		AggType:   ftypes.AggType(popt.AggType),
		Durations: popt.Durations,
		Window:    popt.Window,
		Limit:     popt.Limit,
		Normalize: popt.Normalize,
	}
}

func ToProtoOptions(opt Options) *AggOptions {
	return &AggOptions{
		AggType:      string(opt.AggType),
		Durations:    opt.Durations,
		Window:       opt.Window,
		Limit:        opt.Limit,
		Normalize:    opt.Normalize,
		CronSchedule: opt.CronSchedule,
	}
}
