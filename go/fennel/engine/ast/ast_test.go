package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEquals(t *testing.T) {
	for i, a := range TestExamples {
		for j, b := range TestExamples {
			if i == j {
				assert.True(t, a.Equals(b))
				assert.True(t, b.Equals(a))
			} else {
				assert.False(t, a.Equals(b))
				assert.False(t, b.Equals(a))
			}
		}
	}
}
