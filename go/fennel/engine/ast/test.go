package ast

import (
	"fmt"
	"strconv"
)

var TestExamples []Ast

func MakeInt(i int32) *Atom {
	return &Atom{Type: Int, Lexeme: fmt.Sprintf("%d", i)}
}

func MakeDouble(d float64) *Atom {
	return &Atom{Type: Double, Lexeme: strconv.FormatFloat(d, 'f', -1, 64)}
}

func MakeString(s string) *Atom {
	return &Atom{Type: String, Lexeme: s}
}

func MakeBool(b bool) *Atom {
	var str string
	if b {
		str = "true"
	} else {
		str = "false"
	}
	return &Atom{Type: Bool, Lexeme: str}
}

func MakeUnary(op string, operand Ast) *Unary {
	return &Unary{Op: op, Operand: operand}
}

func MakeBinary(op string, left, right Ast) *Binary {
	return &Binary{Op: op, Left: left, Right: right}
}

func MakeList(values ...Ast) *List {
	if len(values) == 0 {
		values = []Ast{}
	}
	return &List{Values: values}
}

func MakeDict(values map[string]Ast) *Dict {
	if values == nil {
		values = make(map[string]Ast)
	}
	return &Dict{Values: values}
}

func MakeVar(name string) *Var {
	return &Var{Name: name}
}

func MakeIfElse(condition, then, else_ Ast) *IfElse {
	return &IfElse{Condition: condition, ThenDo: then, ElseDo: else_}
}

func MakeLookup(on Ast, property string) *Lookup {
	return &Lookup{
		On:       on,
		Property: property,
	}
}

func MakeStatement(name string, body Ast) *Statement {
	return &Statement{
		Name: name,
		Body: body,
	}
}

func MakeQuery(statements []*Statement) *Query {
	return &Query{Statements: statements}
}

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
		MakeList(),
		MakeList(MakeInt(1), MakeString("hi"), MakeDouble(3.4), MakeBool(true)),
		// list containing list
		MakeList(MakeInt(1), MakeString("hi"), MakeList()),
		MakeDict(map[string]Ast{}),
		MakeDict(map[string]Ast{
			"hi":  MakeBool(false),
			"bye": MakeList(MakeInt(1), MakeString("yo")),
		}),
		&Unary{
			Op:      "~",
			Operand: MakeBool(true),
		},
		&Binary{
			Left:  MakeInt(1),
			Op:    "+",
			Right: MakeBool(false), // syntatically this is fine even if this is an error semantically
		},
		&Binary{
			Left:  &Binary{MakeBool(false), "any random string", MakeInt(1)},
			Op:    "+",
			Right: MakeBool(false), // syntatically this is fine even if this is an error semantically
		},
		&Statement{"x", MakeInt(1)},
		&Query{Statements: []*Statement{
			{"x", MakeInt(1)},
			{"longer name with space", &List{Values: []Ast{}}},
		}},
		&Var{"x"},
		&Var{""},
		&OpCall{
			Operands:  []Ast{MakeInt(1), MakeInt(2)},
			Vars:      []string{"x", "y"},
			Namespace: "my namespace",
			Name:      "my name",
			Kwargs:    MakeDict(nil),
		},
		&IfElse{
			Condition: MakeBool(true),
			ThenDo:    MakeInt(4),
			ElseDo:    MakeInt(7),
		},
		&IfElse{
			Condition: MakeBool(false),
			ThenDo:    MakeInt(9),
			ElseDo:    MakeInt(5),
		},
	}

	lookups := make([]Ast, 0)
	for _, t := range TestExamples {
		lookups = append(lookups, &Lookup{On: t, Property: "someprop"})
		lookups = append(lookups, &Lookup{On: t, Property: "some prop with space"})
	}
	TestExamples = append(TestExamples, lookups...)
}
