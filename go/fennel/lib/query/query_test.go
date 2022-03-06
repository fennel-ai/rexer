package query

import (
	"encoding/base64"
	"fmt"
	"testing"

	"fennel/engine/ast"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestBoundQueryJSON(t *testing.T) {
	type test struct {
		str  string
		tree ast.Ast
		args value.Dict
	}
	var tests []test
	vals := []value.Dict{
		value.Dict(nil),
		{},
		{"k1": value.Nil},
		{"k1": value.Double(3.14), "k2": value.Int(128), "k3": value.String("abc"), "k4": value.Bool(false)},
		{"k1": value.List{value.List{}, value.Dict{}}, "k2": value.Dict{"x": value.List{}}},
	}
	for i, tr := range ast.TestExamples {
		v := vals[i%len(vals)]
		s, err := makeBoundQueryJSON(tr, v)
		assert.NoError(t, err)
		tests = append(tests, test{s, tr, v})
	}

	// Test unmarshal
	for _, tst := range tests {
		tree, args, err := FromBoundQueryJSON([]byte(tst.str))
		assert.NoError(t, err)
		assert.True(t, tst.tree.Equals(tree))
		assert.True(t, tst.args.Equal(args))
	}
	// Test marshal
	for _, tst := range tests {
		// Ast does not serialize to a unique string
		// So test by converting to and from JSON
		ser, err := ToBoundQueryJSON(tst.tree, tst.args)
		assert.NoError(t, err)
		tree, args, err := FromBoundQueryJSON(ser)
		assert.NoError(t, err)
		assert.True(t, tst.tree.Equals(tree))
		assert.True(t, tst.args.Equal(args))
	}
}

func makeBoundQueryJSON(tree ast.Ast, args value.Dict) (string, error) {
	astSer, err := ast.Marshal(tree)
	if err != nil {
		return "", err
	}
	astStr := base64.StdEncoding.EncodeToString(astSer)
	argsSer, err := value.ToJSON(args)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf(`{"Ast":"%s","Args":%s}`, astStr, argsSer), nil
}
