package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

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
		List{Int(1), Bool(false), String("hi"), List{Int(5)}},
		Dict{"a": Int(2), "b": Dict{}, "c": List{}, "d": Double(-4.2)},
		Nil,
	}

	for _, v := range values {
		pv, err := ToProtoValue(v)
		assert.NoError(t, err)
		v2, err := FromProtoValue(&pv)
		assert.NoError(t, err)
		assert.Equal(t, v, v2)

		// also verify futures
		f := getFuture(v)
		pf, err := ToProtoValue(f)
		assert.NoError(t, err)
		assert.Equal(t, pv, pf)
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
