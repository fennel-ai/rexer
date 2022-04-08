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
	if err := inputTable.Append([]value.Value{nil}, value.NewDict(vKwargs)); err != nil {
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
	// query args are present in the root Env (has no parent)
	rootEnv := i.env
	for rootEnv.parent != nil {
		rootEnv = rootEnv.parent
	}
	args, err := rootEnv.Lookup("__args__")
	if err != nil {
		return value.NewDict(map[string]value.Value{})
	}
	asDict, ok := args.(value.Dict)
	if !ok {
		return value.NewDict(map[string]value.Value{})
	}
	return asDict
}

// Eval the given query in separate goroutine after unpacking args
// args are set up in the base environment, which makes it possible for
// user query to create own variables with names which may shadow variables in query args
func (i Interpreter) Eval(query ast.Ast, args value.Dict) (value.Value, error) {
	resch := make(chan value.Value, 1)
	errch := make(chan error, 1)
	go func() {
		ii := NewInterpreter(i.bootargs)
		err := ii.env.Define("__args__", args)
		if err != nil {
			errch <- err
			return
		}
		args := args.Iter()
		for name, val := range args {
			if err = ii.env.Define(name, val); err != nil {
				errch <- err
				return
			}
		}
		// push a new environment on top of base environment
		// this way, user query can define variables with the
		// same names as those in query args which they will now mask
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
		return value.Nil, fmt.Errorf("property '%s' does not exist in the dictionary: '%s'", property, asdict)
	}
	return ret, nil
}

func (i Interpreter) visitInContext(tree ast.Ast, varmap map[string]value.Value) (value.Value, error) {
	i.env = i.env.PushEnv()
	defer func() { i.env, _ = i.env.PopEnv() }()

	for k, v := range varmap {
		if err := i.env.DefineReferencable(k, v); err != nil {
			return value.Nil, err
		}
	}
	return tree.AcceptValue(i)
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

func (i Interpreter) VisitUnary(op string, operand ast.Ast) (value.Value, error) {
	v, err := operand.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	return v.OpUnary(op)
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

func (i Interpreter) VisitOpcall(operands []ast.Ast, vars []string, namespace, name string, kwargs ast.Dict) (value.Value, error) {
	// either vars should be not defined at all or if they are defined, number should match that of operands
	if len(vars) > 0 && len(operands) != len(vars) {
		return nil, fmt.Errorf("operator '%s.%s' can not be applied: different number of operands and variables", namespace, name)
	}
	// eval operands and verify it is of the right type
	voperands := make([]value.List, len(operands))
	for j, operand := range operands {
		val, err := operand.AcceptValue(i)
		if err != nil {
			return value.Nil, err
		}
		inData, ok := val.(value.List)
		if !ok {
			return value.Nil, fmt.Errorf("operator '%s.%s' can not be applied: operand not a list", namespace, name)
		}
		voperands[j] = inData
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

	// and same for inputs + dynamic kwargs to create InputTable
	inputTable, err := i.getContextKwargs(op, kwargs, voperands, vars)
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

func (i Interpreter) getContextKwargs(op operators.Operator, trees ast.Dict, inputs []value.List, vars []string) (operators.ZipTable, error) {
	ret := operators.NewZipTable(op)
	sig := op.Signature()
	// TODO: relax to potentially having zero inputs?
	for j := 0; j < inputs[0].Len(); j++ {
		v := make([]value.Value, len(inputs))
		for idx := range inputs {
			val, err := inputs[idx].At(j)
			if err != nil {
				return operators.ZipTable{}, fmt.Errorf("unequal length of operands")
			}
			v[idx] = val
		}

		// set all the lambda variables as needed
		varmap := make(map[string]value.Value)
		for idx := range vars {
			varname := vars[idx]
			varval, err := inputs[idx].At(j)
			if err != nil {
				return operators.ZipTable{}, fmt.Errorf("unequal length of operands")
			}
			varmap[varname] = varval
		}
		// now using these lambda variables, evaluate kwargs variables
		kwargs := value.NewDict(nil)
		for k, p := range sig.ContextKwargs {
			tree, ok := trees.Values[k]
			switch {
			case !ok && !p.Optional:
				return operators.ZipTable{}, fmt.Errorf("kwarg '%s' not provided for operator '%s.%s'", k, sig.Module, sig.Name)
			case !ok && p.Optional:
				kwargs.Set(k, p.Default)
			case ok:
				val, err := i.visitInContext(tree, varmap)
				if err != nil {
					return operators.ZipTable{}, fmt.Errorf("error: %s while evaluating kwarg '%s' for operator '%s.%s'", err, k, sig.Module, sig.Name)
				}
				kwargs.Set(k, val)
			}
		}
		if err := ret.Append(v, kwargs); err != nil {
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
