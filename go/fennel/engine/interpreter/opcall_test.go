package interpreter

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/ast"
	"fennel/engine/operators"
	"fennel/lib/value"
)

func TestInterpreter_VisitOpcall(t *testing.T) {
	astrow1 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": ast.MakeInt(3),
			"b":       ast.MakeString("hi"),
		},
	}
	astrow2 := ast.Dict{
		Values: map[string]ast.Ast{
			"a.inner": ast.MakeInt(5),
			"b":       ast.MakeString("bye"),
		},
	}
	astrow3 := ast.Dict{
		Values: map[string]ast.Ast{
			"b":       ast.MakeString("hello"),
			"a.inner": ast.MakeInt(3),
		},
	}
	astTable := &ast.List{Values: []ast.Ast{astrow1, astrow2, astrow3}}
	row1, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(3),
		"b":       value.String("hi"),
	})
	row2, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(5),
		"b":       value.String("bye"),
	})
	row3, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(3),
		"b":       value.String("hello"),
	})
	table := value.List{}
	table.Append(row1)
	table.Append(row2)
	table.Append(row3)

	// we get all values back if where is true
	kwargs := ast.Dict{
		Values: map[string]ast.Ast{
			"where": ast.MakeBool(true),
		},
	}
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, table)

	// and we get nothing when filter is passed as "false"
	kwargs = ast.Dict{
		Values: map[string]ast.Ast{
			"where": ast.MakeBool(false),
		},
	}
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, value.List{})

	// and if where is more specific, that works too
	kwargs = ast.Dict{
		Values: map[string]ast.Ast{
			"where": ast.Binary{
				Left:  ast.Lookup{On: ast.At{}, Property: "a.inner"},
				Right: ast.MakeInt(3),
				Op:    "==",
			},
		},
	}
	expected := value.List{}
	expected.Append(row1)
	expected.Append(row3)
	testValid(t, ast.OpCall{
		Operand:   astTable,
		Namespace: "std",
		Name:      "filter",
		Kwargs:    kwargs,
	}, expected)
}

func TestInterpreter_VisitOpcall2(t *testing.T) {
	// here we create nested opcall that contain both static/contextual kwargs
	base := value.List{}
	row1 := value.Dict{"hi": value.Int(1), "bye": value.Double(1)}
	row2 := value.Dict{"hi": value.Int(2), "bye": value.Double(2)}
	row3 := value.Dict{"hi": value.Int(3), "bye": value.Double(3)}
	assert.NoError(t, base.Append(row1))
	assert.NoError(t, base.Append(row2))
	assert.NoError(t, base.Append(row3))
	i := getInterpreter()
	query := getOpCallQuery()
	res, err := i.Eval(query, value.Dict{"table": base})
	assert.NoError(t, err)
	expected := value.List{}
	assert.NoError(t, expected.Append(value.Dict{"hi": value.Int(2), "bye": value.Double(2), "key": value.List{value.Double(2)}}))
	assert.NoError(t, expected.Append(value.Dict{"hi": value.Int(3), "bye": value.Double(3), "key": value.List{value.Double(3)}}))
	assert.Equal(t, expected, res)
}

func TestInterpreter_VisitOpcall3(t *testing.T) {
	// first register the test operation
	operators.Register(&testOpInit{})
	// then create an ast that uses this op
	query := ast.OpCall{
		Operand: ast.Lookup{
			On:       ast.Var{Name: "args"},
			Property: "table",
		},
		Namespace: "test",
		Name:      "op",
		Kwargs:    ast.Dict{},
	}
	table := value.List{}
	table.Append(value.Dict{"x": value.Int(1)})
	nonhi := "hello"
	i := NewInterpreter(map[string]interface{}{
		"__teststruct__": testNonValue{hi: nonhi},
	})
	out, err := i.Eval(query, value.Dict{"num": value.Int(41), "table": table})
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Len(t, rows, 1)
	assert.Equal(t, value.Dict{"x": value.Int(1), "num": value.Int(41), "nonhi": value.String(nonhi)}, rows[0])
}

func TestInterpreter_VisitOpcall4(t *testing.T) {
	operators.Register(testOpDefault{})
	query := ast.OpCall{
		Operand: ast.Lookup{
			On:       ast.Var{Name: "args"},
			Property: "table",
		},
		Namespace: "test",
		Name:      "testop",
		Kwargs:    ast.Dict{},
	}
	table := value.List{}
	table.Append(value.Dict{})
	i := getInterpreter()
	out, err := i.Eval(query, value.Dict{"table": table})
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Len(t, rows, 1)
	assert.Equal(t, value.Dict{"contextual": value.Int(41), "static": value.Int(7)}, rows[0])
}

func TestInterpreter_VisitOpcall5(t *testing.T) {
	// verifies that if the same operator appears twice in a query,
	// it works, even when the operator has some internal state
	operators.Register(&rowCount{})
	query := ast.OpCall{
		Operand: ast.OpCall{
			Operand: ast.Lookup{
				On:       ast.Var{Name: "args"},
				Property: "input",
			},
			Namespace: "test",
			Name:      "row_count",
			Kwargs:    ast.Dict{},
		},
		Namespace: "test",
		Name:      "row_count",
		Kwargs:    ast.Dict{},
	}
	input := value.List{}
	input.Append(value.Int(10))
	input.Append(value.Int(20))
	i := getInterpreter()
	out, err := i.Eval(query, value.Dict{"input": input})
	assert.NoError(t, err)
	rows := out.(value.List)
	assert.Len(t, rows, 4)
	assert.Equal(t, value.List{value.Int(10), value.Int(20), value.Int(2), value.Int(3)}, rows)
}

func TestInterpreter_VisitFnCall(t *testing.T) {
	operators.Register(zip{})
	operators.Register(squareFn{})
	scenarios := []struct {
		query    ast.Ast
		err      bool
		expected value.Value
	}{
		{
			ast.FnCall{
				Module: "test",
				Name:   "zip",
				Kwargs: map[string]ast.Ast{"left": ast.MakeInt(3)},
			}, true, nil,
		},
		{
			query: ast.FnCall{
				Module: "test",
				Name:   "zip",
				Kwargs: map[string]ast.Ast{"left": ast.MakeInt(3), "right": ast.List{Values: []ast.Ast{ast.MakeInt(1)}}},
			}, err: true, expected: nil,
		},
		{
			query: ast.FnCall{
				Module: "test",
				Name:   "zip",
				Kwargs: map[string]ast.Ast{
					"left":  ast.List{[]ast.Ast{ast.MakeInt(1), ast.MakeInt(3)}},
					"right": ast.List{[]ast.Ast{ast.MakeInt(2), ast.MakeString("hi")}},
				}}, err: false,
			expected: value.List{value.List{value.Int(1), value.Int(2)}, value.List{value.Int(3), value.String("hi")}},
		},
		{
			ast.FnCall{
				Module: "test",
				Name:   "square",
				Kwargs: map[string]ast.Ast{"x": ast.MakeInt(3)},
			}, false, value.Int(9),
		},
	}
	for _, scene := range scenarios {
		i := getInterpreter()
		found, err := i.Eval(scene.query, value.Dict{})
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.expected, found)
		}
	}
}

type testOpDefault struct{}

func (t testOpDefault) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return testOpDefault{}, nil
}

func (t testOpDefault) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		rowVal, context, _ := in.Next()
		row := rowVal.(value.Dict)
		row["contextual"] = context["contextual"]
		row["static"] = kwargs["static"]
		out.Append(row)
	}
	return nil
}

func (t testOpDefault) Signature() *operators.Signature {
	return operators.NewSignature("test", "testop", true).
		Input(value.Types.Dict).
		Param("contextual", value.Types.Int, false, true, value.Int(41)).
		Param("static", value.Types.Int, true, true, value.Int(7))
}

var _ operators.Operator = testOpDefault{}

type testOpInit struct {
	num value.Int
	non testNonValue
}
type testNonValue struct {
	hi string
}

var _ operators.Operator = testOpInit{}

func (top testOpInit) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	// take one arg from args and one from bootarg to verify that init is working
	num, ok := args["num"]
	if !ok {
		return nil, fmt.Errorf("num not passed")
	}
	return testOpInit{
		num: num.(value.Int),
		non: bootargs["__teststruct__"].(testNonValue),
	}, nil
}

func (top testOpInit) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		rowVal, _, _ := in.Next()
		row := rowVal.(value.Dict)
		row["num"] = top.num
		row["nonhi"] = value.String(top.non.hi)
		out.Append(row)
	}
	return nil
}

func (top testOpInit) Signature() *operators.Signature {
	return operators.NewSignature("test", "op", false).Input(value.Types.Dict)
}

type rowCount struct {
	num int
}

func (r *rowCount) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return &rowCount{}, nil
}

func (r *rowCount) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	for in.HasMore() {
		v, _, _ := in.Next()
		r.num += 1
		out.Append(v)
	}
	out.Append(value.Int(r.num))
	return nil
}

func (r *rowCount) Signature() *operators.Signature {
	return operators.NewSignature("test", "row_count", false)
}

var _ operators.Operator = &rowCount{}

type squareFn struct{}

func (s squareFn) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return squareFn{}, nil
}

func (s squareFn) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	_, kwargs, err := in.Next()
	if err != nil {
		return err
	}
	switch n := kwargs["x"].(type) {
	case value.Int:
		out.Append(n * n)
	case value.Double:
		out.Append(n * n)
	}
	return nil
}

func (s squareFn) Signature() *operators.Signature {
	return operators.NewSignature("test", "square", true).
		Param("x", value.Types.Number, false, false, value.Int(0))
}

var _ operators.Operator = squareFn{}

func benchmarkInterpreter_VisitOpcall(numRows int, b *testing.B) {
	table := value.List{}
	for i := 0; i < numRows; i++ {
		row := value.Dict{"hi": value.Int(i), "bye": value.Double(i)}
		table.Append(row)
	}
	evaler := getInterpreter()
	query := getOpCallQuery()
	for i := 0; i < b.N; i++ {
		res, _ = evaler.Eval(query, value.Dict{"table": table})
	}
}

type zip struct{}

func (e zip) New(args value.Dict, bootargs map[string]interface{}) (operators.Operator, error) {
	return zip{}, nil
}

func (e zip) Apply(kwargs value.Dict, in operators.InputIter, out *value.List) error {
	_, kwargs, err := in.Next()
	if err != nil {
		return err
	}
	left, right := kwargs["left"].(value.List), kwargs["right"].(value.List)
	if len(left) != len(right) {
		return fmt.Errorf("unequal lengths")
	}
	ret := value.List{}
	for i := range left {
		ret.Append(value.List{left[i], right[i]})
	}
	return out.Append(ret)
}

func (e zip) Signature() *operators.Signature {
	return operators.NewSignature("test", "zip", false).
		Param("left", value.Types.List, false, false, nil).
		Param("right", value.Types.List, false, false, nil)
}

var _ operators.Operator = zip{}

func BenchmarkInterpreter_VisitOpcall100(b *testing.B) {
	benchmarkInterpreter_VisitOpcall(100, b)
}

func BenchmarkInterpreter_VisitOpcall1K(b *testing.B) {
	benchmarkInterpreter_VisitOpcall(1000, b)
}

func BenchmarkInterpreter_VisitOpcall10K(b *testing.B) {
	benchmarkInterpreter_VisitOpcall(10000, b)
}
