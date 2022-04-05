package std

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestFlattenOperator_Apply(t *testing.T) {
	type scenario struct {
		inputs   []value.Value
		static   value.Dict
		context  []value.Dict
		expected []value.Value
	}
	zDict := value.NewDict(map[string]value.Value{})
	vInt := value.Int(-3)
	vDouble := value.Double(2.5)
	vString := value.String("abc")
	vBool := value.Bool(false)
	vDict := value.NewDict(map[string]value.Value{"key": value.NewList(vString, vBool)})
	l0 := value.NewList(vInt, vDouble, vDict)
	l1 := value.NewList(vInt, l0)
	l2 := value.NewList(vDouble, l1)

	scenarios := []scenario{
		{
			[]value.Value{l2, l2},
			value.NewDict(map[string]value.Value{"depth": value.Int(0)}),
			[]value.Dict{zDict, zDict},
			[]value.Value{vDouble, vInt, vInt, vDouble, vDict, vDouble, vInt, vInt, vDouble, vDict},
		},
		{
			[]value.Value{l2, l2},
			value.NewDict(map[string]value.Value{"depth": value.Int(1)}),
			[]value.Dict{zDict, zDict},
			[]value.Value{vDouble, vInt, l0, vDouble, vInt, l0},
		},
		{
			[]value.Value{l2, l2},
			value.NewDict(map[string]value.Value{"depth": value.Int(2)}),
			[]value.Dict{zDict, zDict},
			[]value.Value{vDouble, vInt, vInt, vDouble, vDict, vDouble, vInt, vInt, vDouble, vDict},
		},
		{
			[]value.Value{l2, l2},
			value.NewDict(map[string]value.Value{"depth": value.Int(3)}),
			[]value.Dict{zDict, zDict},
			[]value.Value{vDouble, vInt, vInt, vDouble, vDict, vDouble, vInt, vInt, vDouble, vDict},
		},
		{
			[]value.Value{l2, l2},
			value.NewDict(map[string]value.Value{}),
			[]value.Dict{zDict, zDict},
			[]value.Value{vDouble, vInt, vInt, vDouble, vDict, vDouble, vInt, vInt, vDouble, vDict},
		},
	}

	tr := tier.Tier{}
	for _, scene := range scenarios {
		optest.AssertEqual(t, tr, FlattenOperator{}, scene.static, scene.inputs, scene.context, scene.expected)
	}
	// negative depth fails
	optest.AssertError(t, tr, FlattenOperator{},
		value.NewDict(map[string]value.Value{"depth": value.Int(-1)}), scenarios[0].inputs, scenarios[0].context)
}
