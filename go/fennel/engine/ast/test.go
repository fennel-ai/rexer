package ast

import (
	"fmt"
	"strconv"
)

func MakeInt(i int32) Atom {
	return Atom{Type: Int, Lexeme: fmt.Sprintf("%d", i)}
}

func MakeDouble(d float64) Atom {
	return Atom{Type: Double, Lexeme: strconv.FormatFloat(d, 'f', -1, 64)}
}

func MakeString(s string) Atom {
	return Atom{Type: String, Lexeme: s}
}

func MakeBool(b bool) Atom {
	var str string
	if b {
		str = "true"
	} else {
		str = "false"
	}
	return Atom{Type: Bool, Lexeme: str}
}

var TestExamples = []Ast{
	MakeInt(4),
	MakeBool(false),
	MakeBool(true),
	MakeDouble(3.4),
	MakeDouble(-3.4),
	MakeString("hi_123!2!@#"),
	List{[]Ast{}},
	List{Values: []Ast{MakeInt(1), MakeString("hi"), MakeDouble(3.4), MakeBool(true)}},
	// list containing list
	List{Values: []Ast{MakeInt(1), MakeString("hi"), List{[]Ast{}}}},
	Dict{Values: map[string]Ast{}},
	Dict{Values: map[string]Ast{
		"hi":  MakeBool(false),
		"bye": List{Values: []Ast{MakeInt(1), MakeString("yo")}},
	}},
	Binary{
		Left:  MakeInt(1),
		Op:    "+",
		Right: MakeBool(false), // syntatically this is fine even if this is an error semantically
	},
	Binary{
		Left:  Binary{MakeBool(false), "any random string", MakeInt(1)},
		Op:    "+",
		Right: MakeBool(false), // syntatically this is fine even if this is an error semantically
	},
	Statement{"x", MakeInt(1)},
	Query{Statements: []Statement{
		{"x", MakeInt(1)},
		{"longer name with space", List{Values: []Ast{}}},
	}},
	Var{"x"},
	Var{""},
	Table{Inner: MakeInt(1)}, // again semantic error but fine syntactically
	Table{Inner: List{Values: []Ast{
		Dict{map[string]Ast{
			"int":  MakeInt(1),
			"bool": MakeBool(false),
		}},
		Dict{map[string]Ast{
			"int":  MakeInt(3),
			"bool": MakeBool(true),
		}},
	}}}, // again semantic error but fine syntactically
	OpCall{
		Operand:   Table{Inner: MakeInt(1)},
		Namespace: "my namespace",
		Name:      "my name",
		Kwargs:    Dict{Values: map[string]Ast{}},
	},
}
