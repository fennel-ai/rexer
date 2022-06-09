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
	args value.Dict, bootargs map[string]interface{},
) (Operator, error) {
	return nil, nil
}

func (top testOpZip) Apply(_ context.Context, kwargs Kwargs, in InputIter, out *value.List) error {
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
	context1, err := NewKwargs(op.Signature(), []value.Value{value.Double(3.0), value.String("bye")}, false)
	assert.NoError(t, err)
	row2 := value.NewDict(map[string]value.Value{
		"a": value.Int(9),
		"b": value.String("third"),
	})
	context2, err := NewKwargs(op.Signature(), []value.Value{value.Double(122.0), value.String("fourth")}, false)
	assert.NoError(t, err)
	err = zt.Append([]value.Value{row1}, context1)
	assert.NoError(t, err)
	assert.Equal(t, 1, zt.Len())
	err = zt.Append([]value.Value{row2}, context2)
	assert.NoError(t, err)
	assert.Equal(t, 2, zt.Len())
}

func TestIterTypeCheck(t *testing.T) {
	t.Parallel()
	op := testOpZip{}
	scenarios := []struct {
		rows   value.List
		kwargs []Kwargs
		errs   []bool
		name   string
	}{
		{value.NewList(value.String("hello"), value.String("again")),
			[]Kwargs{
				{sig: op.Signature(), static: false, vals: []value.Value{value.Double(3.0), value.Nil}},
				{sig: op.Signature(), static: false, vals: []value.Value{value.Double(12.1), value.Int(2)}},
			},
			[]bool{false, false},
			"basic",
		},
		{value.NewList(value.String("hello"), value.Int(3)),
			[]Kwargs{
				{sig: op.Signature(), static: false, vals: []value.Value{value.Double(3.0), value.Nil}},
				{sig: op.Signature(), static: false, vals: []value.Value{value.Double(12.1), value.Int(2)}},
			},
			[]bool{false, true},
			"basic_input_mistyping",
		},
		{value.NewList(value.Nil, value.Int(3)),
			[]Kwargs{
				{sig: op.Signature(), static: false, vals: []value.Value{value.Double(3.0), value.Nil}},
				{sig: op.Signature(), static: false, vals: []value.Value{value.Double(12.1), value.Int(2)}},
			},
			[]bool{true, true},
			"basic_input_mistyping_2",
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
