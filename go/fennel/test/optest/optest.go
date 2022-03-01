package optest

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/operators"
	"fennel/lib/value"
)

func Assert(t *testing.T, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Dict) {
	iter := operators.NewZipTable(op)
	for i, row := range inputs {
		assert.NoError(t, iter.Append(row, context[i]))
	}
	found := value.List{}
	assert.NoError(t, op.Apply(static, iter.Iter(), &found))
	//found := outtable.Pull()
	assert.Len(t, found, len(expected))
	assert.ElementsMatch(t, expected, found)
}

func AssertError(t *testing.T, op operators.Operator, static value.Dict, inputs, context []value.Dict) {
	iter := operators.NewZipTable(op)
	for i, row := range inputs {
		assert.NoError(t, iter.Append(row, context[i]))
	}
	outtable := value.List{}
	assert.Error(t, op.Apply(static, iter.Iter(), &outtable))
}
