package operators

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/lib/value"
)

type testOpZip struct {
}

var _ Operator = testOp{}

func (top testOpZip) New(
	args *value.Dict, bootargs map[string]interface{},
) (Operator, error) {
	return nil, nil
}

func (top testOpZip) Apply(_ context.Context, kwargs *value.Dict, in InputIter, out *value.List) error {
	return nil
}

func (top testOpZip) Signature() *Signature {
	return NewSignature("test", "op").
		Param("p1", value.Types.Bool, true, false, value.Nil).
		Param("p2", value.Types.Double, false, false, value.Double(3.0)).
		Param("p3", value.Types.Any, false, false, value.Nil).
		Input([]value.Type{value.Types.String})
}

func TestNewZipTable(t *testing.T) {
	t.Parallel()
	op := testOpZip{}
	zt := NewZipTable(op)
	assert.Equal(t, 0, zt.Len())
	row1 := value.NewDict(map[string]value.Value{
		"a": value.Int(1),
		"b": value.String("hi"),
	})
	row2 := value.NewDict(map[string]value.Value{
		"a": value.Int(5),
		"b": value.String("bye"),
	})
	row3 := value.NewDict(map[string]value.Value{
		"a": value.Int(9),
		"b": value.String("third"),
	})
	row4 := value.NewDict(map[string]value.Value{
		"a": value.Int(122),
		"b": value.String("fourt"),
	})
	err := zt.Append([]value.Value{row1}, row2)
	assert.NoError(t, err)
	assert.Equal(t, 1, zt.Len())
	err = zt.Append([]value.Value{row3}, row4)
	assert.NoError(t, err)
	assert.Equal(t, 2, zt.Len())
}

func TestIterTypeCheck(t *testing.T) {
	t.Parallel()
	op := testOpZip{}
	scenarios := []struct {
		rows   value.List
		kwargs []*value.Dict
		errs   []bool
		name   string
	}{
		{value.NewList(value.String("hello"), value.String("again")),
			[]*value.Dict{value.NewDict(map[string]value.Value{"p2": value.Double(3.0), "p3": value.Nil}), value.NewDict(map[string]value.Value{"p2": value.Double(12.1), "p3": value.Int(2)})},
			[]bool{false, false},
			"basic",
		},
		{value.NewList(value.String("hello"), value.Int(3)),
			[]*value.Dict{value.NewDict(map[string]value.Value{"p2": value.Double(3.0), "p3": value.Nil}), value.NewDict(map[string]value.Value{"p2": value.Double(12.1), "p3": value.Int(2)})},
			[]bool{false, true},
			"basic_input_mistyping",
		},
		{value.NewList(value.Nil, value.Int(3)),
			[]*value.Dict{value.NewDict(map[string]value.Value{"p2": value.Double(3.0), "p3": value.Nil}), value.NewDict(map[string]value.Value{"p2": value.Double(12.1), "p3": value.Int(2)})},
			[]bool{true, true},
			"basic_input_mistyping_2",
		},
		{value.NewList(value.String("hello"), value.String("again")),
			[]*value.Dict{value.NewDict(map[string]value.Value{"p2": value.Int(3.0), "p3": value.Nil}), value.NewDict(map[string]value.Value{"p2": value.Double(12.1), "p3": value.Int(2)})},
			[]bool{true, false},
			"basic_kwarg_mistyping",
		},
	}

	for _, scenario := range scenarios {
		zt := NewZipTable(op)
		for i := 0; i < scenario.rows.Len(); i++ {
			v, _ := scenario.rows.At(i)
			assert.NoError(t, zt.Append([]value.Value{v}, scenario.kwargs[i]), scenario.name)
		}
		iter := zt.Iter()
		for i := 0; i < scenario.rows.Len(); i++ {
			assert.True(t, iter.HasMore(), scenario.name)
			row, kwargs, err := iter.Next()
			if scenario.errs[i] {
				assert.Error(t, err, scenario.name)
			} else {
				assert.NoError(t, err, scenario.name)
				v, _ := scenario.rows.At(i)
				assert.Equal(t, []value.Value{v}, row, scenario.name)
				assert.Equal(t, scenario.kwargs[i], kwargs, scenario.name)
			}
		}
	}
}
