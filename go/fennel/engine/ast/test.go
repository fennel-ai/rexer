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

var TestExamples []Ast

func init() {
	// This should not contain duplicates
	// Used in ast_test.go to check if each element
	// is equal to only itself.
	TestExamples = []Ast{
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
		OpCall{
			Operands:  []Ast{MakeInt(1), MakeInt(2)},
			Vars:      []string{"x", "y"},
			Namespace: "my namespace",
			Name:      "my name",
			Kwargs:    Dict{Values: map[string]Ast{}},
		},
		At{},
		IfElse{
			Condition: MakeBool(true),
			ThenDo:    MakeInt(4),
			ElseDo:    MakeInt(7),
		},
		IfElse{
			Condition: MakeBool(false),
			ThenDo:    MakeInt(9),
			ElseDo:    MakeInt(5),
		},
		FnCall{
			Module: "std",
			Name:   "something",
			Kwargs: map[string]Ast{
				"hi":        MakeBool(false),
				"something": List{Values: []Ast{MakeInt(1), MakeDouble(3.4)}},
			},
		},
		FnCall{
			Module: "std",
			Name:   "something",
			Kwargs: map[string]Ast{
				"hi":        Dict{Values: map[string]Ast{}},
				"something": List{Values: []Ast{MakeInt(1), MakeDouble(3.4)}},
			},
		},
		FnCall{
			Module: "std",
			Name:   "something",
			Kwargs: map[string]Ast{},
		},
		HighFnCall{
			Type:    Map,
			Varname: "x",
			Lambda:  Var{Name: "x"},
			Iter:    List{[]Ast{}},
		},
		HighFnCall{
			Type:    Map,
			Varname: "x",
			Lambda: Binary{
				Left:  Var{"x"},
				Op:    "*",
				Right: MakeInt(2),
			},
			Iter: List{Values: []Ast{MakeInt(1), MakeInt(3)}},
		},
	}

	lookups := make([]Ast, 0)
	for _, t := range TestExamples {
		lookups = append(lookups, Lookup{On: t, Property: "someprop"})
		lookups = append(lookups, Lookup{On: t, Property: "some prop with space"})
	}
	TestExamples = append(TestExamples, lookups...)
}
