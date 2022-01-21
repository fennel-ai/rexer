package ast

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

func makeInt(i int32) Atom {
	return Atom{Type: Int, Lexeme: fmt.Sprintf("%d", i)}
}

func makeDouble(d float64) Atom {
	return Atom{Type: Double, Lexeme: strconv.FormatFloat(d, 'f', -1, 64)}
}

func makeString(s string) Atom {
	return Atom{Type: String, Lexeme: s}
}

func makeBool(b bool) Atom {
	var str string
	if b {
		str = "true"
	} else {
		str = "false"
	}
	return Atom{Type: Bool, Lexeme: str}
}

func check(t *testing.T, valid []Ast, invalid []Ast) {
	for _, test := range valid {
		past, err := ToProtoAst(test)
		assert.NoError(t, err)
		found, err := FromProtoAst(past)
		assert.NoError(t, err)
		assert.Equal(t, test, found, test)
	}
	for _, test := range invalid {
		_, err := ToProtoAst(test)
		assert.Error(t, err, test)
	}
}

func TestFromProtoAst(t *testing.T) {
	valid := []Ast{
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
	invalid := []Ast{
		Atom{Type: Int, Lexeme: "bool"}, // this should fail at conversion to proto
		nil,
		Table{Inner: nil},
		List{[]Ast{makeBool(false), nil, makeBool(true)}},
	}
	check(t, valid, invalid)
}
