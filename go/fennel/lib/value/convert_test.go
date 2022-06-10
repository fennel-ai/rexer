package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertToCapnValue(t *testing.T) {
	values := []Value{
		Int(1),
		Int(-12),
		Bool(true),
		Bool(false),
		String("this is a string"),
		String(""),
		Double(1.0),
		Double(0.0),
		Double(1e0),
		Double(-1e0),
		NewList(),
		NewList(Int(1), Bool(false), String("hi"), NewList(Int(5))),
		NewDict(map[string]Value{"a": Int(2), "b": NewDict(map[string]Value{}), "c": NewList(), "d": Double(-4.2)}),
		Nil,
	}

	for _, v := range values {
		cv, err := ToCapnValue(v)
		assert.NoError(t, err)
		v2, err := FromCapnValue(cv)
		assert.NoError(t, err)
		assert.Equal(t, v, v2)
	}
}

func TestConvert(t *testing.T) {
	values := []Value{
		Int(1),
		Int(-12),
		Bool(true),
		Bool(false),
		String("this is a string"),
		String(""),
		Double(1.0),
		Double(0.0),
		Double(1e0),
		Double(-1e0),
		NewList(Int(1), Bool(false), String("hi"), NewList(Int(5))),
		NewDict(map[string]Value{"a": Int(2), "b": NewDict(map[string]Value{}), "c": NewList(), "d": Double(-4.2)}),
		Nil,
	}

	for _, v := range values {
		pv, err := ToProtoValue(v)
		assert.NoError(t, err)
		v2, err := FromProtoValue(&pv)
		assert.NoError(t, err)
		assert.Equal(t, v, v2)
	}
}

func TestInvalidProtoValue(t *testing.T) {
	empty := PValue{}
	pvalues := []*PValue{
		// a protovalue without a valid type
		&empty,
		{
			// a protovalue containing a list containing a protovalue without a valid type
			Node: &PValue_List{List: &PVList{Values: []*PValue{&empty}}},
		},
		{
			// a protovalue containing a dict containing a protovalue without a valid type
			Node: &PValue_Dict{Dict: &PVDict{Values: map[string]*PValue{"hi": &empty}}},
		},
	}

	for _, pv := range pvalues {
		_, err := FromProtoValue(pv)
		assert.Error(t, err)
	}
}
