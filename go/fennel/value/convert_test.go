package value

import (
	"fennel/value/proto"
	"github.com/stretchr/testify/assert"
	"testing"
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
	table := NewTable()
	table.Append(Dict{"a": Int(2), "b": Bool(false), "l": List{Int(1), Int(2)}})
	table.Append(Dict{"a": Int(6), "b": Bool(true), "l": List{Int(3), Int(2)}})
	values = append(values, table)

	for _, v := range values {
		pv, err := ToProtoValue(v)
		assert.NoError(t, err)
		v2, err := FromProtoValue(&pv)
		assert.NoError(t, err)
		assert.Equal(t, v, v2)
	}
}

func TestInvalidProtoValue(t *testing.T) {
	empty := proto.PValue{}
	pvalues := []proto.PValue{
		// a protovalue without a valid type
		empty,
		{
			// a protovalue containing a list containing a protovalue without a valid type
			Node: &proto.PValue_List{List: &proto.PVList{Values: []*proto.PValue{&empty}}},
		},
		{
			// a protovalue containing a dict containing a protovalue without a valid type
			Node: &proto.PValue_Dict{Dict: &proto.PVDict{Values: map[string]*proto.PValue{"hi": &empty}}},
		},
	}

	// a protovalue table where schema doesn't match
	row1, _ := ToProtoDict(Dict{"hi": Int(1), "bye": Bool(true)})
	row2, _ := ToProtoDict(Dict{"mismatch": Int(1), "bye": Bool(true)})
	ptable := proto.PValue{Node: &proto.PValue_Table{Table: &proto.PVTable{Rows: []*proto.PVDict{&row1, &row2}}}}

	pvalues = append(pvalues, ptable)

	for _, pv := range pvalues {
		_, err := FromProtoValue(&pv)
		assert.Error(t, err)
	}
}
