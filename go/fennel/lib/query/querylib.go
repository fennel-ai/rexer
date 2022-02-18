package query

import (
	"encoding/base64"
	"encoding/json"
	"fennel/engine/ast"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fmt"
	"github.com/buger/jsonparser"
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
	Args value.Dict
}

func FromProtoBoundQuery(pbq *ProtoBoundQuery) (ast.Ast, value.Dict, error) {
	tree, err := ast.FromProtoAst(*pbq.Ast)
	if err != nil {
		return nil, value.Dict{}, err
	}
	args, err := value.FromProtoDict(pbq.Dict)
	if err != nil {
		return nil, value.Dict{}, err
	}

	return tree, args, nil
}

func ToProtoBoundQuery(bq *BoundQuery) (ProtoBoundQuery, error) {
	ast, err := ast.ToProtoAst(bq.Ast)
	if err != nil {
		return ProtoBoundQuery{}, err
	}
	dict, err := value.ToProtoDict(bq.Args)
	if err != nil {
		return ProtoBoundQuery{}, err
	}
	return ProtoBoundQuery{
		Ast:  &ast,
		Dict: &dict,
	}, nil
}

func FromBoundQueryJSON(data []byte) (ast.Ast, value.Dict, error) {
	// Extract the ast first
	astStr, err := jsonparser.GetString(data, "Ast")
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing ast json: %v", err)
	}
	astSer, err := base64.StdEncoding.DecodeString(astStr)
	if err != nil {
		return nil, nil, fmt.Errorf("error decoding ast from base64: %v", err)
	}
	var tree ast.Ast
	err = ast.Unmarshal(astSer, &tree)
	if err != nil {
		return nil, nil, fmt.Errorf("error unmarshalling ast: %v", err)
	}
	// Now extract args
	vdata, vtype, _, err := jsonparser.Get(data, "Args")
	if err != nil {
		return nil, nil, fmt.Errorf("error getting args: %v", err)
	}
	argsVar, err := value.ParseJSON(vdata, vtype)
	if err != nil {
		return nil, nil, fmt.Errorf("error parsing args json: %v", err)
	}
	args, ok := argsVar.(value.Dict)
	if !ok {
		return nil, nil, fmt.Errorf("expected value Dict but found: %v", argsVar)
	}
	return tree, args, nil
}

func ToBoundQueryJSON(tree ast.Ast, args value.Dict) ([]byte, error) {
	astSer, err := ast.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("error marshalling ast: %v", err)
	}
	astStr := base64.StdEncoding.EncodeToString(astSer)
	bq := struct {
		Ast  string     `json:"Ast"`
		Args value.Dict `json:"Args"`
	}{Ast: astStr, Args: args}
	return json.Marshal(bq)
}
