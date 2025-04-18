package interpreter

import (
	"context"
	"fennel/lib/arena"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"fennel/engine/ast"
	"fennel/engine/operators"
	"fennel/lib/value"

	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"
)

type Interpreter struct {
	env      *Env
	bootargs map[string]interface{}
	ctx      context.Context
}

func NewInterpreter(ctx context.Context, bootargs map[string]interface{}, args value.Dict) (*Interpreter, error) {
	env := NewEnv(nil)
	if err := env.Define("__args__", args); err != nil {
		return nil, err
	}
	for name, val := range args.Iter() {
		if err := env.Define(name, val); err != nil {
			return nil, fmt.Errorf("could not define arg '%s' %v", name, err)
		}
	}
	// Push a new environment on top of base environment.
	// This way, user query can define variables with the
	// same names as those in query args which they will now mask
	env = NewEnv(env)
	return &Interpreter{
		env:      env,
		bootargs: bootargs,
		ctx:      ctx,
	}, nil
}

var _ ast.VisitorValue = (*Interpreter)(nil)

func (i *Interpreter) queryArgs() value.Dict {
	// query args are present in the root Env (has no parent)
	rootEnv := i.env
	for rootEnv.parent != nil {
		rootEnv = rootEnv.parent
	}
	args, err := rootEnv.Lookup("__args__")
	if err != nil {
		return value.NewDict(nil)
	}
	asDict, ok := args.(value.Dict)
	if !ok {
		return value.NewDict(nil)
	}
	return asDict
}

func (i *Interpreter) VisitLookup(on ast.Ast, property string) (value.Value, error) {
	val, err := on.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	asdict, ok := val.(value.Dict)
	if !ok {
		return value.Nil, fmt.Errorf("can not lookup property '%s' on non-dict value: '%s'", property, val.String())
	}
	ret, ok := asdict.Get(property)
	if !ok {
		return value.Nil, fmt.Errorf("property '%s' does not exist in the dictionary: '%s'", property, asdict)
	}
	return ret, nil
}

func (i *Interpreter) visitInContext(tree ast.Ast, vars []string, varvals []value.Value) (value.Value, error) {
	i.env = i.env.PushEnv()
	defer func() { i.env, _ = i.env.PopEnv() }()

	for idx, k := range vars {
		if err := i.env.DefineReferencable(k, varvals[idx]); err != nil {
			return value.Nil, err
		}
	}
	return tree.AcceptValue(i)
}

func (i *Interpreter) VisitStatement(name string, body ast.Ast) (value.Value, error) {
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

func (i *Interpreter) VisitQuery(statements []*ast.Statement) (value.Value, error) {
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

func (i *Interpreter) VisitAtom(at ast.AtomType, lexeme string) (value.Value, error) {
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

func (i *Interpreter) VisitUnary(op string, operand ast.Ast) (value.Value, error) {
	v, err := operand.AcceptValue(i)
	if err != nil {
		return value.Nil, err
	}
	return v.OpUnary(op)
}

func (i *Interpreter) VisitBinary(left ast.Ast, op string, right ast.Ast) (value.Value, error) {
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

func (i *Interpreter) VisitList(values []ast.Ast) (value.Value, error) {
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

func (i *Interpreter) VisitDict(values map[string]ast.Ast) (value.Value, error) {
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

func (i *Interpreter) VisitOpcall(operands []ast.Ast, vars []string, namespace, name string, kwargs *ast.Dict) (value.Value, error) {
	if len(operands) == 0 {
		return value.Nil, fmt.Errorf("operator '%s.%s' can not be applied: no operands", namespace, name)
	}
	// either vars should be not defined at all or if they are defined, number should match that of operands
	if len(vars) > 0 && len(operands) != len(vars) {
		return nil, fmt.Errorf("operator '%s.%s' can not be applied: different number of operands and variables", namespace, name)
	}
	cCtx, span := otel.Tracer("fennel").Start(i.ctx, fmt.Sprintf("%s.%s", namespace, name))
	defer span.End()

	// eval all operands
	vals, err := i.visitAll(operands, cCtx)
	if err != nil {
		return value.Nil, err
	}
	// verify each operand is a list
	voperands := make([]value.List, len(operands))
	for j := range vals {
		val := vals[j]
		inData, ok := val.(value.List)
		if !ok {
			return value.Nil, fmt.Errorf("operator '%s.%s' can not be applied because operand '%s' not a list", namespace, name, val)
		}
		voperands[j] = inData
	}
	szOperands := voperands[0].Len()

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
	// and same for inputs + dynamic kwargs to create InputTable
	// just pre-create space for all context kwargs
	kwargVals := arena.Values.Alloc(0, szOperands*len(op.Signature().ContextKwargs))
	defer arena.Values.Free(kwargVals)
	inputTable, err := i.getContextKwargs(op, kwargs, voperands, vars, kwargVals)
	if err != nil {
		return value.Nil, err
	}
	// finally, call the operator
	// typing of input / context kwargs is verified element by element inside the iter
	outtable := value.NewList()
	outtable.Grow(inputTable.Len())
	if err = op.Apply(cCtx, staticKwargs, inputTable.Iter(), &outtable); err != nil {
		return value.Nil, err
	}
	return outtable, nil
}

func (i *Interpreter) VisitVar(name string) (value.Value, error) {
	return i.env.Lookup(name)
}

func (i *Interpreter) VisitIfelse(condition ast.Ast, thenDo ast.Ast, elseDo ast.Ast) (value.Value, error) {
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
		return value.Nil, fmt.Errorf("invalid cond: condition '%s' does not evaluate to a boolean", condition)
	}
}

type RequiredKwargNotProvidedError struct {
	ParamName string
	OpModule  string
	OpName    string
}

func (e *RequiredKwargNotProvidedError) Error() string {
	return fmt.Sprintf("kwarg '%s' not provided for operator '%s.%s'", e.ParamName, e.OpModule, e.OpName)
}

func (e *RequiredKwargNotProvidedError) Is(targetError error) bool {
	if te, ok := targetError.(*RequiredKwargNotProvidedError); ok {
		return e.ParamName == te.ParamName && e.OpModule == te.OpModule && e.OpName == te.OpName
	}
	return false
}

func (i *Interpreter) getStaticKwargs(op operators.Operator, kwargs *ast.Dict) (operators.Kwargs, error) {
	vals := make([]value.Value, 0, len(kwargs.Values))
	sig := op.Signature()
	for _, p := range sig.StaticKwargs {
		k := p.Name
		tree, ok := kwargs.Values[k]
		switch {
		case !ok && !p.Optional:
			return operators.Kwargs{}, &RequiredKwargNotProvidedError{ParamName: k, OpModule: sig.Module, OpName: sig.Name}
		case !ok && p.Optional:
			vals = append(vals, p.Default)
		case ok:
			val, err := tree.AcceptValue(i)
			if err != nil {
				return operators.Kwargs{}, fmt.Errorf("error while evaluating kwarg '%s' for operator '%s.%s': %s", k, sig.Module, sig.Name, err)
			}
			vals = append(vals, val)
		}
	}
	return operators.NewKwargs(sig, vals, true)
}

func (i *Interpreter) getContextKwargs(op operators.Operator, trees *ast.Dict, inputs []value.List, vars []string, kwargVals []value.Value) (operators.ZipTable, error) {
	ret := operators.NewZipTable(op)
	sig := op.Signature()
	varvals := make([]value.Value, len(vars))
	// allocate all the values of all operands together
	data := make([]value.Value, len(inputs)*inputs[0].Len())
	ptr := 0
	ret.Grow(inputs[0].Len())

	err := validateOperandLength(inputs, sig)
	if err != nil {
		return operators.ZipTable{}, err
	}

	for j := 0; j < inputs[0].Len(); j++ {
		begin := ptr
		for idx := range inputs {
			val, err := inputs[idx].At(j)
			if err != nil {
				return operators.ZipTable{}, fmt.Errorf("error while evaluating operand %d for operator '%s.%s': %s", idx, sig.Module, sig.Name, err)
			}
			data[ptr] = val
			ptr++
			// set all the lambda variables as needed
			if len(vars) > idx {
				varvals[idx] = val
			}
		}
		// now using these lambda variables, evaluate kwargs variables
		kwargValBegin := len(kwargVals)
		for _, p := range sig.ContextKwargs {
			k := p.Name
			tree, ok := trees.Values[k]
			switch {
			case !ok && !p.Optional:
				return ret, &RequiredKwargNotProvidedError{ParamName: k, OpModule: sig.Module, OpName: sig.Name}
			case !ok && p.Optional:
				kwargVals = append(kwargVals, p.Default)
				continue
			case ok:
				// we have to evaluate the tree with the current values of the lambda variables
				val, done, err := i.fastKwargEval(tree, vars, varvals)
				if done {
					if err != nil {
						return operators.ZipTable{}, fmt.Errorf(
							"error while evaluating kwarg '%s' for operator '%s.%s': %s", k, sig.Module, sig.Name, err,
						)
					}
					kwargVals = append(kwargVals, val)
				} else {
					val, err := i.visitInContext(tree, vars, varvals)
					if err != nil {
						return operators.ZipTable{}, fmt.Errorf(
							"error while evaluating kwarg '%s' for operator '%s.%s': %s", k, sig.Module, sig.Name, err,
						)
					}
					kwargVals = append(kwargVals, val)
				}
			}
		}
		kwargs, err := operators.NewKwargs(sig, kwargVals[kwargValBegin:], false)
		if err != nil {
			return ret, err
		}
		if err := ret.Append(data[begin:ptr], kwargs); err != nil {
			return operators.ZipTable{}, err
		}
	}
	return ret, nil
}

func (i *Interpreter) fastKwargEval(tree ast.Ast, vars []string, varvals []value.Value) (value.Value, bool, error) {
	// a common scenario is to evaluate an atom (e.g. user writing "user" as otype in profile)
	// in that case, we can avoid setting the lambda variables, which also saves function call
	if atom, ok := tree.(*ast.Atom); ok {
		val, err := atom.AcceptValue(i)
		return val, true, err
	}
	// another common case is a property lookup on the first variable
	if lookup, ok := tree.(*ast.Lookup); ok && len(vars) > 0 {
		if var_, ok := lookup.On.(*ast.Var); ok && var_.Name == vars[0] {
			asdict, ok := varvals[0].(value.Dict)
			if !ok {
				return nil, true, fmt.Errorf("property %s does not exist on dictionary", lookup.Property)
			}
			val, found := asdict.Get(lookup.Property)
			if found {
				return val, true, nil
			} else {
				return nil, true, fmt.Errorf("property %s does not exist on dictionary", lookup.Property)
			}
		}
	}
	return nil, false, nil
}

func (i *Interpreter) getOperator(namespace, name string) (operators.Operator, error) {
	op, err := operators.Locate(namespace, name)
	if err != nil {
		return op, err
	}
	ret, err := op.New(i.queryArgs(), i.bootargs)
	return ret, err
}

func (i *Interpreter) visitAll(trees []ast.Ast, ctx context.Context) ([]value.Value, error) {
	tracer := otel.Tracer("fennel")
	cCtx, span := tracer.Start(ctx, "operandsVisit")
	defer span.End()
	span.SetAttributes(attribute.Int("numSubTrees", len(trees)))

	vals := make([]value.Value, len(trees))
	var err error
	if len(trees) == 1 {
		// Create a new interpreter to pass the new context used in the trace
		subtreeInterpreter := Interpreter{i.env, i.bootargs, cCtx}
		vals[0], err = trees[0].AcceptValue(&subtreeInterpreter)
	} else {
		// Eval trees in parallel if more than 1.
		eg := errgroup.Group{}
		for j := range trees {
			idx := j
			eg.Go(func() error {
				// Copy interpreter here, so the go-routines don't share the
				// same Env except the current one.
				subtreeCtx, subtreeSpan := tracer.Start(cCtx, fmt.Sprintf("subtree_%d", idx))
				defer subtreeSpan.End()
				subtreeInterpreter := Interpreter{i.env, i.bootargs, subtreeCtx}
				var err error
				vals[idx], err = trees[idx].AcceptValue(&subtreeInterpreter)
				return err
			})
		}
		err = eg.Wait()
	}
	return vals, err
}

func validateOperandLength(inputs []value.List, sig *operators.Signature) error {
	incorrectOperandIndex := -1
	for k := range inputs {
		if inputs[k].Len() != inputs[0].Len() {
			incorrectOperandIndex = k
			break
		}
	}

	if incorrectOperandIndex != -1 {
		lengthOfOperands := make([]string, len(inputs))
		for k := range inputs {
			lengthOfOperands[k] = fmt.Sprint(inputs[k].Len())
		}
		return fmt.Errorf("operator '%s.%s' can not be applied: unequal lengths of operands %s", sig.Module, sig.Name, strings.Join(lengthOfOperands, ", "))
	}
	return nil
}
