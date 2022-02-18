package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"strings"
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
		Options:   &agg.Options,
	}, nil
}

func FromProtoAggregate(pagg ProtoAggregate) (Aggregate, error) {
	query, err := ast.FromProtoAst(*pagg.Query)
	if err != nil {
		return Aggregate{}, err
	}
	agg := Aggregate{
		Name:      ftypes.AggName(strings.ToLower(pagg.AggName)),
		Query:     query,
		Timestamp: ftypes.Timestamp(pagg.Timestamp),
		Options:   *pagg.Options,
	}
	agg.Options.AggType = strings.ToLower(agg.Options.AggType)
	return agg, nil
}
