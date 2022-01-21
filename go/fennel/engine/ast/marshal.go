package ast

import (
	astProto "fennel/engine/ast/proto"
	"google.golang.org/protobuf/proto"
)

func Marshal(ast Ast) ([]byte, error) {
	pa, err := ToProtoAst(ast)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&pa)
}

func Unmarshal(data []byte, ast *Ast) error {
	var pa astProto.Ast
	if err := proto.Unmarshal(data, &pa); err != nil {
		return err
	}
	a, err := FromProtoAst(pa)
	if err != nil {
		return err
	}
	*ast = a
	return nil
}
