package interpreter

import (
	"fennel/engine/ast"
	"fennel/engine/operators"
	"fennel/value"
	"fmt"
	"reflect"
)

type Interpreter struct {
	env Env
}

var _ ast.VisitorValue = Interpreter{}

func (i Interpreter) VisitStatement(name string, body *ast.Ast) (value.Value, error) {
	val, err := body.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	if name != "" {
		err = i.env.Define(name, val)
		if err != nil {
			return value.Nil, err
		}
	}
	return val, nil
}

func (i Interpreter) VisitQuery(statements []*ast.Statement) (value.Value, error) {
	if len(statements) == 0 {
		return value.Nil, fmt.Errorf("query can not be empty")
	}
	var exp value.Value
	var err error
	for _, statement := range statements {
		exp, err = statement.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
	}
	return exp, nil
}

func (i Interpreter) VisitAtom(a *ast.Atom) (value.Value, error) {
	switch a.Inner.(type) {
	case *ast.Atom_Int:
		return value.Int(a.GetInt()), nil
	case *ast.Atom_Double:
		return value.Double(a.GetDouble()), nil
	case *ast.Atom_Bool:
		return value.Bool(a.GetBool()), nil
	case *ast.Atom_String_:
		return value.String(a.GetString_()), nil
	}
	panic("unreachable code")
}

func (i Interpreter) VisitBinary(left *ast.Ast, op string, right *ast.Ast) (value.Value, error) {
	// TODO: short-circuit for bool and/or
	l, err := left.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	r, err := right.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	return l.Op(op, r)
}

func (i Interpreter) VisitList(values []*ast.Ast) (value.Value, error) {
	ret := make([]value.Value, 0, len(values))
	for _, v := range values {
		val, err := v.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		ret = append(ret, val)
	}
	return value.NewList(ret)
}

func (i Interpreter) VisitDict(values map[string]*ast.Ast) (value.Value, error) {
	ret := make(map[string]value.Value, len(values))
	for k, v := range values {
		val, err := v.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		ret[k] = val
	}
	return value.NewDict(ret)
}

func (i Interpreter) VisitTable(inner *ast.Ast) (value.Value, error) {
	rows, err := inner.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}

	switch rows.(type) {
	case value.List:
		ret := value.NewTable()
		for _, elem := range rows.(value.List) {
			switch elem.(type) {
			case value.Dict:
				err = ret.Append(elem.(value.Dict))
				if err != nil {
					return value.Nil, fmt.Errorf("table can only be created via list of dicts with same schema")
				}
			default:
				return value.Nil, fmt.Errorf("table can only be created via list of dicts")
			}
		}
		return ret, nil
	case value.Table:
		return rows.(value.Table).Clone(), nil
	default:
		return value.Nil, fmt.Errorf("table can only be created via list of dicts")
	}
}

func (i Interpreter) VisitOpcall(operand *ast.Ast, namespace, name string, kwargs *ast.Dict) (value.Value, error) {
	// eval operand and verify it is of the right type
	val, err := operand.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	switch val.(type) {
	case value.Table:
	default:
		return value.Nil, fmt.Errorf("opertor '%s.%s' can not be applied: operand not a table", namespace, name)
	}
	intable := val.(value.Table)

	// now eval kwargs and verify they are of the right type
	kw, err := kwargs.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	switch kw.(type) {
	case value.Dict:
	default:
		return value.Nil, fmt.Errorf("kwargs should be a dictionary but found :'%s'", kw.String())
	}
	kwdict := kw.(value.Dict)

	// locate the operator
	op, err := operators.Locate(namespace, name)
	if err != nil {
		return value.Nil, err
	}

	// verify typing of all kwargs
	// TODO: pass table's real schema, not just empty schema
	if err = operators.Validate(op, kwdict, map[string]reflect.Type{}); err != nil {
		return value.Nil, err
	}
	// finally, call the operator
	outtable := value.NewTable()
	if err = op.Apply(kwdict, intable, &outtable); err != nil {
		return value.Nil, err
	}
	return outtable, nil
}

func (i Interpreter) VisitVar(name string) (value.Value, error) {
	return i.env.Lookup(name)
}
