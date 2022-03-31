package query

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"fennel/controller/mock"
	"github.com/buger/jsonparser"

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
	Args value.Dict
	Mock mock.Data
}

func FromProtoBoundQuery(pbq *ProtoBoundQuery) (ast.Ast, value.Dict, error) {
	tree, err := ast.FromProtoAst(pbq.Ast)
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

func FromBoundQueryJSON(data []byte) (tree ast.Ast, args value.Dict, mockData mock.Data, err error) {
	// Extract the ast first
	astStr, err := jsonparser.GetString(data, "Ast")
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error parsing ast json: %v", err)
	}
	astSer, err := base64.StdEncoding.DecodeString(astStr)
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error decoding ast from base64: %v", err)
	}
	err = ast.Unmarshal(astSer, &tree)
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error unmarshalling ast: %v", err)
	}
	// Now extract args
	vdata, vtype, _, err := jsonparser.Get(data, "Args")
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error getting args: %v", err)
	}
	argsVar, err := value.ParseJSON(vdata, vtype)
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error parsing args json: %v", err)
	}
	args, ok := argsVar.(value.Dict)
	if !ok {
		return tree, args, mockData, fmt.Errorf("expected value Dict but found: %v", argsVar)
	}
	// Now get mock data
	vdata, _, _, err = jsonparser.Get(data, "Mock")
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error getting mock: %v", err)
	}
	err = json.Unmarshal(vdata, &mockData)
	if err != nil {
		return tree, args, mockData, fmt.Errorf("error parsing mock json: %v", err)
	}
	return tree, args, mockData, nil
}

func ToBoundQueryJSON(tree ast.Ast, args value.Dict, mockData mock.Data) ([]byte, error) {
	astSer, err := ast.Marshal(tree)
	if err != nil {
		return nil, fmt.Errorf("error marshalling ast: %v", err)
	}
	astStr := base64.StdEncoding.EncodeToString(astSer)
	bq := struct {
		Ast  string     `json:"Ast"`
		Args value.Dict `json:"Args"`
		Mock mock.Data  `json:"Mock"`
	}{Ast: astStr, Args: value.Clean(args).(value.Dict), Mock: mockData}
	return json.Marshal(bq)
}
