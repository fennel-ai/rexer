package query

import (
	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
)

type QueryRequest struct {
	QueryId      uint64           `db:"query_id"`
	MinTimestamp ftypes.Timestamp `db:"min_timestamp"`
	MaxTimestamp ftypes.Timestamp `db:"max_timestamp"`
}

type QuerySer struct {
	QueryId   uint64           `db:"query_id"`
	Timestamp ftypes.Timestamp `db:"timestamp"`
	QuerySer  string           `db:"query_ser"`
}

type BoundQuery struct {
	Ast  ast.Ast
	Dict value.Dict
}

func FromProtoBoundQuery(pbq *ProtoBoundQuery) (ast.Ast, value.Dict, error) {
	astvar, err := ast.FromProtoAst(*pbq.Ast)
	if err != nil {
		return nil, value.Dict{}, err
	}
	dict, err := value.FromProtoDict(pbq.Dict)
	if err != nil {
		return nil, value.Dict{}, err
	}

	return astvar, dict, nil
}

func ToProtoBoundQuery(bq *BoundQuery) (ProtoBoundQuery, error) {
	ast, err := ast.ToProtoAst(bq.Ast)
	if err != nil {
		return ProtoBoundQuery{}, err
	}
	dict, err := value.ToProtoDict(bq.Dict)
	if err != nil {
		return ProtoBoundQuery{}, err
	}
	return ProtoBoundQuery{
		Ast:  &ast,
		Dict: &dict,
	}, nil
}
