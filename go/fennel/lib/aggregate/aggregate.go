package aggregate

import (
	"fennel/engine/ast"
	"fennel/lib/ftypes"
)

type Aggregate struct {
	CustID    ftypes.CustID
	Type      ftypes.AggType
	Name      ftypes.AggName
	Query     ast.Ast
	Timestamp ftypes.Timestamp
	Options   AggOptions
}

func ToProtoAggregate(agg Aggregate) (ProtoAggregate, error) {
	pquery, err := ast.ToProtoAst(agg.Query)
	if err != nil {
		return ProtoAggregate{}, err
	}
	return ProtoAggregate{
		CustId:    uint64(agg.CustID),
		AggType:   string(agg.Type),
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
	return Aggregate{
		CustID:    ftypes.CustID(pagg.CustId),
		Type:      ftypes.AggType(pagg.AggType),
		Name:      ftypes.AggName(pagg.AggName),
		Query:     query,
		Timestamp: ftypes.Timestamp(pagg.Timestamp),
		Options:   *pagg.Options,
	}, nil
}

type AggregateSer struct {
	CustID    ftypes.CustID    `db:"cust_id"`
	Type      ftypes.AggType   `db:"aggregate_type"`
	Name      ftypes.AggName   `db:"name"`
	QuerySer  []byte           `db:"query_ser"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	OptionSer []byte           `db:"options_ser"`
}
