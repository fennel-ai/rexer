package interpreter

import (
	"engine/ast"
	"engine/runtime"
	"fmt"
	"strconv"
)

type Interpreter struct {
	env runtime.Env
}

var _ ast.VisitorValue = Interpreter{}

func (i Interpreter) VisitStatement(name string, body ast.Ast) (runtime.Value, error) {
	val, err := body.AcceptValue(i)
	if err != nil {
		return runtime.Nil, err
	}
	if name != "" {
		err = i.env.Define(name, val)
		if err != nil {
			return runtime.Nil, err
		}
	}
	return val, nil
}

func (i Interpreter) VisitQuery(statements []ast.Statement) (runtime.Value, error) {
	if len(statements) == 0 {
		return runtime.Nil, fmt.Errorf("query can not be empty")
	}
	var exp runtime.Value
	var err error
	for _, statement := range statements {
		exp, err = statement.AcceptValue(i)
		if err != nil {
			return runtime.Nil, err
		}
	}
	return exp, nil
}

func (i Interpreter) VisitAtom(at ast.AtomType, lexeme string) (runtime.Value, error) {
	switch at {
	case ast.Int:
		n, err := strconv.Atoi(lexeme)
		if err == nil {
			return runtime.Int(n), nil
		} else {
			return runtime.Nil, err
		}
	case ast.Double:
		f, err := strconv.ParseFloat(lexeme, 64)
		if err == nil {
			return runtime.Double(f), nil
		} else {
			return runtime.Nil, err
		}
	case ast.Bool:
		f, err := strconv.ParseBool(lexeme)
		if err == nil {
			return runtime.Bool(f), nil
		} else {
			return runtime.Nil, err
		}
	case ast.String:
		return runtime.String(lexeme), nil
	}
	panic("unreachable code")
}

func (i Interpreter) VisitBinary(left ast.Ast, op string, right ast.Ast) (runtime.Value, error) {
	l, err := left.AcceptValue(i)
	if err != nil {
		return runtime.Nil, err
	}
	r, err := right.AcceptValue(i)
	if err != nil {
		return runtime.Nil, err
	}
	return l.Op(op, r)
}

func (i Interpreter) VisitList(values []ast.Ast) (runtime.Value, error) {
	ret := make([]runtime.Value, 0, len(values))
	for _, v := range values {
		val, err := v.AcceptValue(i)
		if err != nil {
			return runtime.Nil, err
		}
		ret = append(ret, val)
	}
	return runtime.NewList(ret)
}

func (i Interpreter) VisitDict(values map[string]ast.Ast) (runtime.Value, error) {
	ret := make(map[string]runtime.Value, len(values))
	for k, v := range values {
		val, err := v.AcceptValue(i)
		if err != nil {
			return runtime.Nil, err
		}
		ret[k] = val
	}
	return runtime.NewDict(ret)
}

func (i Interpreter) VisitTable(inner ast.Ast) (runtime.Value, error) {
	//TODO implement me
	panic("implement me")
}

func (i Interpreter) VisitOpcall(operand ast.Ast, namespace, name string, kwargs ast.Dict) (runtime.Value, error) {
	//TODO implement me
	panic("implement me")
}

func (i Interpreter) VisitVar(name string) (runtime.Value, error) {
	return i.env.Lookup(name)
}
