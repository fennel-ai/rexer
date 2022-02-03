package query

import (
	"fennel/engine/ast"
	"fennel/lib/value"
)

type AstWithDict struct {
	Ast  ast.Ast
	Dict value.Dict
}

func FromProtoAstWithDict(pawd *ProtoAstWithDict) (ast.Ast, value.Dict, error) {
	astvar, err := ast.FromProtoAst(*pawd.Ast)
	if err != nil {
		return nil, value.Dict{}, err
	}
	dict, err := value.FromProtoDict(pawd.Dict)
	if err != nil {
		return nil, value.Dict{}, err
	}

	return astvar, dict, nil
}

func ToProtoAstWithDict(awd *AstWithDict) (ProtoAstWithDict, error) {
	ast, err := ast.ToProtoAst(awd.Ast)
	if err != nil {
		return ProtoAstWithDict{}, nil
	}
	dict, err := value.ToProtoDict(awd.Dict)
	if err != nil {
		return ProtoAstWithDict{}, nil
	}
	return ProtoAstWithDict{
		Ast:  &ast,
		Dict: &dict,
	}, nil
}
