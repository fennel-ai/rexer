package ast

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMarshal(t *testing.T) {
	tests := []Ast{
		makeInt(4),
		makeBool(false),
		makeBool(true),
		makeDouble(3.4),
		makeDouble(-3.4),
		makeString("hi_123!2!@#"),
		List{[]Ast{}},
		List{Values: []Ast{makeInt(1), makeString("hi"), makeDouble(3.4), makeBool(true)}},
		// list containing list
		List{Values: []Ast{makeInt(1), makeString("hi"), List{[]Ast{}}}},
		Dict{Values: map[string]Ast{}},
		Dict{Values: map[string]Ast{
			"hi":  makeBool(false),
			"bye": List{Values: []Ast{makeInt(1), makeString("yo")}},
		}},
		Binary{
			Left:  makeInt(1),
			Op:    "+",
			Right: makeBool(false), // syntatically this is fine even if this is an error semantically
		},
		Binary{
			Left:  Binary{makeBool(false), "any random string", makeInt(1)},
			Op:    "+",
			Right: makeBool(false), // syntatically this is fine even if this is an error semantically
		},
		Statement{"x", makeInt(1)},
		Query{statements: []Statement{
			{"x", makeInt(1)},
			{"longer name with space", List{Values: []Ast{}}},
		}},
		Var{"x"},
		Var{""},
		Table{Inner: makeInt(1)}, // again semantic error but fine syntactically
		Table{Inner: List{Values: []Ast{
			Dict{map[string]Ast{
				"int":  makeInt(1),
				"bool": makeBool(false),
			}},
			Dict{map[string]Ast{
				"int":  makeInt(3),
				"bool": makeBool(true),
			}},
		}}}, // again semantic error but fine syntactically
		OpCall{
			Operand:   Table{Inner: makeInt(1)},
			Namespace: "my namespace",
			Name:      "my name",
			Kwargs:    Dict{Values: map[string]Ast{}},
		},
	}
	for _, test := range tests {
		data, err := Marshal(test)
		assert.NoError(t, err)
		var found Ast
		err = Unmarshal(data, &found)
		assert.NoError(t, err)
		assert.Equal(t, test, found)
	}
}
