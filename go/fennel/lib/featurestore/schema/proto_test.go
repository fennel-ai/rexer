package schema

import (
	"testing"

	"fennel/lib/featurestore/schema/proto"
	"github.com/stretchr/testify/assert"
)

func TestSerializeDeserialize(t *testing.T) {
	schemas := []Schema{
		{
			Name:   "empty",
			Fields: []Field{},
		},
		{
			Name: "one",
			Fields: []Field{
				{
					Name:     "timestamp",
					DataType: ScalarType(proto.ScalarType_TIMESTAMP),
					Nullable: false,
					DefaultValue: Timestamp{
						Seconds: 0,
						Nanos:   0,
						Now:     false,
					},
				},
			},
		},
		{
			Name: "s1",
			Fields: []Field{
				{
					Name:         "id",
					DataType:     ScalarType(proto.ScalarType_INT),
					Nullable:     false,
					DefaultValue: Int(0),
				},
				{
					Name:         "name",
					DataType:     ScalarType(proto.ScalarType_STRING),
					Nullable:     false,
					DefaultValue: String(""),
				},
				{
					Name:         "amount",
					DataType:     ScalarType(proto.ScalarType_DOUBLE),
					Nullable:     false,
					DefaultValue: Double(0.0),
				},
				{
					Name:         "isMember",
					DataType:     ScalarType(proto.ScalarType_BOOLEAN),
					Nullable:     false,
					DefaultValue: Bool(false),
				},
				{
					Name: "array",
					DataType: ArrayType{ValueType: MapType{
						KeyType:   ScalarType(proto.ScalarType_STRING),
						ValueType: ScalarType(proto.ScalarType_TIMESTAMP),
					}},
					Nullable:     true,
					DefaultValue: Array{Elements: []Value{}},
				},
				{
					Name: "map",
					DataType: MapType{
						KeyType:   ScalarType(proto.ScalarType_INT),
						ValueType: ArrayType{ValueType: ScalarType(proto.ScalarType_DOUBLE)},
					},
					Nullable: true,
					DefaultValue: Map{
						Keys:   []Value{},
						Values: []Value{},
					},
				},
			},
		},
	}
	for _, sch := range schemas {
		psch := ToProto(sch)
		sch2 := FromProto(psch)
		assert.Equal(t, sch2, sch)
	}
}
