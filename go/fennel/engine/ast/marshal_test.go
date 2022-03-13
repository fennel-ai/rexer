package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshal(t *testing.T) {
	for _, test := range TestExamples {
		data, err := Marshal(test)
		assert.NoError(t, err)
		var found Ast
		err = Unmarshal(data, &found)
		assert.NoError(t, err)
		assert.Equal(t, test, found, test)
	}
}
