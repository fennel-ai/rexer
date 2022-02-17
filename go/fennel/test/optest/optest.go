package optest

import (
	"fennel/engine/operators"
	"fennel/lib/utils"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Assert(t *testing.T, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Dict) {
	iter := utils.NewZipTable()
	for i, row := range inputs {
		assert.NoError(t, iter.Append(row, context[i]))
	}
	outtable := value.NewTable()
	assert.NoError(t, op.Apply(static, iter.Iter(), &outtable))
	found := outtable.Pull()
	assert.Len(t, found, len(expected))
	assert.ElementsMatch(t, expected, found)
}

func AssertError(t *testing.T, op operators.Operator, static value.Dict, inputs, context []value.Dict) {
	iter := utils.NewZipTable()
	for i, row := range inputs {
		assert.NoError(t, iter.Append(row, context[i]))
	}
	outtable := value.NewTable()
	assert.Error(t, op.Apply(static, iter.Iter(), &outtable))
}
