package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func check(t *testing.T, valid []Ast, invalid []Ast) {
	for _, test := range valid {
		past, err := ToProtoAst(test)
		assert.NoError(t, err)
		found, err := FromProtoAst(&past)
		assert.NoError(t, err)
		assert.Equal(t, test, found, test)
	}
	for _, test := range invalid {
		_, err := ToProtoAst(test)
		assert.Error(t, err, test)
	}
}

func TestFromProtoAst(t *testing.T) {
	valid := TestExamples
	invalid := []Ast{
		&Atom{Type: Int, Lexeme: "bool"}, // this should fail at conversion to proto
		nil,
		MakeList(MakeBool(false), nil, MakeBool(true)),
		&IfElse{Condition: MakeBool(true), ThenDo: nil, ElseDo: MakeInt(5)},
		&IfElse{Condition: MakeBool(true), ThenDo: MakeInt(4), ElseDo: nil},
	}
	check(t, valid, invalid)
}
