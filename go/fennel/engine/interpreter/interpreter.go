package interpreter

import (
	"fmt"
	"strconv"
	"strings"

	"fennel/engine/ast"
	"fennel/engine/operators"
	"fennel/lib/value"
)

type Interpreter struct {
	env      *Env
	bootargs map[string]interface{}
}

func (i Interpreter) VisitHighFnCall(Type ast.HighFnType, varname string, lambda ast.Ast, iter ast.Ast) (value.Value, error) {
	viter, err := iter.AcceptValue(i)
	if err != nil {
		return nil, err
	}
	list, ok := viter.(value.List)
	if !ok {
		return nil, fmt.Errorf("%s can only be applied on lists but got: '%s' instead", Type, iter)
	}
	switch Type {
	case ast.Map:
		return i.visitMap(varname, lambda, list)
	case ast.Filter:
		return i.visitFilter(varname, lambda, list)
	default:
		return nil, fmt.Errorf("unsupported higher order function: %s", Type)
	}
}

func (i Interpreter) visitMap(varname string, lambda ast.Ast, list value.List) (value.Value, error) {
	out := value.NewList()
	for j := 0; j < list.Len(); j++ {
		v, _ := list.At(j)
		res, err := i.visitInContext(lambda, varname, v)
		if err != nil {
			return nil, err
		}
		if err = out.Append(res); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (i Interpreter) visitFilter(varname string, lambda ast.Ast, list value.List) (value.Value, error) {
	out := value.NewList()
	for j := 0; j < list.Len(); j++ {
		v, _ := list.At(j)
		res, err := i.visitInContext(lambda, varname, v)
		if err != nil {
			return nil, err
		}
		include, ok := res.(value.Bool)
		if !ok {
			return nil, fmt.Errorf("lambda expression in filter should evaluate to bool but got: '%s' instead", res)
		}
		if include {
			if err = out.Append(v); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func (i Interpreter) VisitFnCall(module, name string, kwargs map[string]ast.Ast) (value.Value, error) {
	// find & init the operator
	op, err := i.getOperator(module, name)
	if err != nil {
		return value.Nil, err
	}
	// now eval  kwargs and verify they are of the right type
	vKwargs := make(map[string]value.Value, len(kwargs))
	for k, ast := range kwargs {
		if v, err := ast.AcceptValue(i); err != nil {
			return nil, err
		} else {
			vKwargs[k] = v
		}
	}
	inputTable := operators.NewZipTable(op)
	if err := inputTable.Append(value.Nil, value.NewDict(vKwargs)); err != nil {
		return nil, err
	}
	// finally, call the function
	// typing of input / context kwargs is verified element by element inside the iter
	outtable := value.NewList()
	if err = op.Apply(value.NewDict(map[string]value.Value{}), inputTable.Iter(), &outtable); err != nil {
		return value.Nil, err
	}
	if outtable.Len() != 1 {
		return nil, fmt.Errorf("function did not return the value: %v", outtable)
	}
	return outtable.At(0)
}

var _ ast.VisitorValue = Interpreter{}

func NewInterpreter(bootargs map[string]interface{}) Interpreter {
	env := NewEnv(nil)
	ret := Interpreter{&env, bootargs}
	return ret
}

func (i Interpreter) queryArgs() value.Dict {
	args, err := i.env.Lookup("args")
	if err != nil {
		return value.Dict{}
	}
	asdict, ok := args.(value.Dict)
	if !ok {
		return value.Dict{}
	}
	return asdict
}

// Eval the given query in separate goroutine after setting Var("args") -> args
// args are set up in the base environment, which makes it possible for
// user query to create own variable called "args" which may shadow query args
func (i Interpreter) Eval(query ast.Ast, args value.Dict) (value.Value, error) {
	resch := make(chan value.Value, 1)
	errch := make(chan error, 1)
	go func() {
		ii := NewInterpreter(i.bootargs)
		if err := ii.env.Define("args", args); err != nil {
			errch <- err
			return
		}
		// push a new environment on top of base environment
		// this way, user query can define a variable called "args" if they want to
		// which will mask the query args
		ii.env = ii.env.PushEnv()
		res, err := query.AcceptValue(ii)
		if err != nil {
			errch <- err
		} else {
			resch <- res
		}
	}()
	select {
	case res := <-resch:
		return res, nil
	case err := <-errch:
		return nil, err
	}
}

func (i Interpreter) VisitLookup(on ast.Ast, property string) (value.Value, error) {
	val, err := on.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	asdict, ok := val.(value.Dict)
	if !ok {
		return value.Nil, fmt.Errorf("can only do property lookup: %s on non-dict value: '%s'", property, on)
	}
	ret, ok := asdict.Get(property)
	if !ok {
		return value.Nil, fmt.Errorf("property: %s does not exist in the dictionary: '%s'", property, asdict)
	}
	return ret, nil
}

func (i Interpreter) visitInContext(tree ast.Ast, varname string, v value.Value) (value.Value, error) {
	i.env = i.env.PushEnv()
	defer func() { i.env, _ = i.env.PopEnv() }()

	if err := i.env.Define(varname, v); err != nil {
		return value.Nil, err
	}
	return tree.AcceptValue(i)
}

func (i Interpreter) VisitAt() (value.Value, error) {
	return i.env.Lookup("@")
}

func (i Interpreter) VisitStatement(name string, body ast.Ast) (value.Value, error) {
	val, err := body.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	if strings.HasPrefix(name, "__") && strings.HasSuffix(name, "__") {
		return value.Nil, fmt.Errorf("variable names starting and ending with '__' are reserved for RQL internals")
	}
	if name != "" {
		err = i.env.Define(name, val)
		if err != nil {
			return value.Nil, err
		}
	}
	return val, nil
}

func (i Interpreter) VisitQuery(statements []ast.Statement) (value.Value, error) {
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

func (i Interpreter) VisitAtom(at ast.AtomType, lexeme string) (value.Value, error) {
	switch at {
	case ast.Int:
		n, err := strconv.Atoi(lexeme)
		if err == nil {
			return value.Int(n), nil
		} else {
			return value.Nil, err
		}
	case ast.Double:
		f, err := strconv.ParseFloat(lexeme, 64)
		if err == nil {
			return value.Double(f), nil
		} else {
			return value.Nil, err
		}
	case ast.Bool:
		f, err := strconv.ParseBool(lexeme)
		if err == nil {
			return value.Bool(f), nil
		} else {
			return value.Nil, err
		}
	case ast.String:
		return value.String(lexeme), nil
	default:
		return value.Nil, fmt.Errorf("invalid atom type: %v", at)
	}
}

func (i Interpreter) VisitBinary(left ast.Ast, op string, right ast.Ast) (value.Value, error) {
	l, err := left.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	if op == "and" && l.Equal(value.Bool(false)) {
		return value.Bool(false), nil
	}
	if op == "or" && l.Equal(value.Bool(true)) {
		return value.Bool(true), nil
	}
	r, err := right.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	return l.Op(op, r)
}

func (i Interpreter) VisitList(values []ast.Ast) (value.Value, error) {
	ret := make([]value.Value, 0, len(values))
	for _, v := range values {
		val, err := v.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		ret = append(ret, val)
	}
	return value.NewList(ret...), nil
}

func (i Interpreter) VisitDict(values map[string]ast.Ast) (value.Value, error) {
	ret := make(map[string]value.Value, len(values))
	for k, v := range values {
		val, err := v.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		ret[k] = val
	}
	return value.NewDict(ret), nil
}

func (i Interpreter) VisitOpcall(operand ast.Ast, namespace, name string, kwargs ast.Dict) (value.Value, error) {
	// eval operand and verify it is of the right type
	val, err := operand.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	inData, ok := val.(value.List)
	if !ok {
		return value.Nil, fmt.Errorf("operator '%s.%s' can not be applied: operand not a list", namespace, name)
	}

	// find & init the operator
	op, err := i.getOperator(namespace, name)
	if err != nil {
		return value.Nil, err
	}
	// now eval static kwargs and verify they are of the right type
	staticKwargs, err := i.getStaticKwargs(op, kwargs)
	if err != nil {
		return value.Nil, err
	}
	if err = operators.TypeCheckStaticKwargs(op, staticKwargs); err != nil {
		return value.Nil, err
	}

	// and same for dynamic kwargs to create InputTable
	inputTable, err := i.getContextKwargs(op, kwargs, inData)
	if err != nil {
		return value.Nil, err
	}
	// finally, call the operator
	// typing of input / context kwargs is verified element by element inside the iter
	outtable := value.NewList()
	if err = op.Apply(staticKwargs, inputTable.Iter(), &outtable); err != nil {
		return value.Nil, err
	}
	return outtable, nil
}

func (i Interpreter) VisitVar(name string) (value.Value, error) {
	return i.env.Lookup(name)
}

func (i Interpreter) VisitIfelse(condition ast.Ast, thenDo ast.Ast, elseDo ast.Ast) (value.Value, error) {
	cond, err := condition.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	if cond.Equal(value.Bool(true)) {
		t, err := thenDo.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		return t, nil
	} else if cond.Equal(value.Bool(false)) {
		e, err := elseDo.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		return e, nil
	} else {
		return value.Nil, fmt.Errorf("condition %s does not evaluate to a boolean", condition)
	}
}

func (i Interpreter) getStaticKwargs(op operators.Operator, kwargs ast.Dict) (value.Dict, error) {
	ret := value.NewDict(map[string]value.Value{})
	sig := op.Signature()
	for k, p := range sig.StaticKwargs {
		tree, ok := kwargs.Values[k]
		switch {
		case !ok && !p.Optional:
			return value.Dict{}, fmt.Errorf("kwarg '%s' not provided for operator '%s.%s'", k, sig.Module, sig.Name)
		case !ok && p.Optional:
			ret.Set(k, p.Default)
			//ret[k] = p.Default
		case ok:
			val, err := tree.AcceptValue(i)
			if err != nil {
				return value.Dict{}, fmt.Errorf("error: %s while evaluating kwarg: %s for operator '%s.%s'", err, k, sig.Module, sig.Name)
			}
			ret.Set(k, val)
		}
	}
	return ret, nil
}

func (i Interpreter) getContextKwargs(op operators.Operator, trees ast.Dict, table value.List) (operators.ZipTable, error) {
	ret := operators.NewZipTable(op)
	sig := op.Signature()
	for j := 0; j < table.Len(); j++ {
		v, _ := table.At(j)
		kwargs := make(map[string]value.Value)
		for k, p := range sig.ContextKwargs {
			tree, ok := trees.Values[k]
			switch {
			case !ok && !p.Optional:
				return operators.ZipTable{}, fmt.Errorf("kwarg '%s' not provided for operator '%s.%s'", k, sig.Module, sig.Name)
			case !ok && p.Optional:
				kwargs[k] = p.Default
			case ok:
				val, err := i.visitInContext(tree, "@", v)
				if err != nil {
					return operators.ZipTable{}, fmt.Errorf("error: %s while evaluating kwarg: %s for operator '%s.%s'", err, k, sig.Module, sig.Name)
				}
				kwargs[k] = val
			}
		}
		dict := value.NewDict(kwargs)
		//if err != nil {
		//	return operators.ZipTable{}, err
		//}
		if err := ret.Append(v, dict); err != nil {
			return operators.ZipTable{}, err
		}
	}
	return ret, nil
}

func (i Interpreter) getOperator(namespace, name string) (operators.Operator, error) {
	op, err := operators.Locate(namespace, name)
	if err != nil {
		return op, err
	}
	ret, err := op.New(i.queryArgs(), i.bootargs)
	return ret, err
}
