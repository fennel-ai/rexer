package ast

import (
	"fennel/engine/ast/proto"
	"fennel/lib/value"
)

type AstWithDict struct {
	Ast  Ast
	Dict value.Dict
}

func FromProtoAstWithDict(pawd *proto.AstWithDict) (AstWithDict, error) {
	ast, err := FromProtoAst(*pawd.Ast)
	if err != nil {
		return AstWithDict{}, err
	}
	dict, err := value.FromProtoDict(pawd.Dict)
	if err != nil {
		return AstWithDict{}, err
	}

	return AstWithDict{
		Ast:  ast,
		Dict: dict,
	}, nil
}

func ToProtoAstWithDict(awd *AstWithDict) (proto.AstWithDict, error) {
	ast, err := ToProtoAst(awd.Ast)
	if err != nil {
		return proto.AstWithDict{}, nil
	}
	dict, err := value.ToProtoDict(awd.Dict)
	if err != nil {
		return proto.AstWithDict{}, nil
	}
	return proto.AstWithDict{
		Ast:  &ast,
		Dict: &dict,
	}, nil
}
