package stack

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStack(t *testing.T) {
	stack := New[int](2)
	for i := 0; i < 10; i++ {
		stack.Push(i)
		top, err := stack.Top()
		assert.NoError(t, err)
		assert.Equal(t, i, top)
		assert.Equal(t, i+1, stack.Len())
	}
	for i := 9; i >= 0; i-- {
		top, err := stack.Pop()
		assert.NoError(t, err)
		assert.Equal(t, i, top)
		assert.Equal(t, i, stack.Len())
	}
	_, err := stack.Top()
	assert.Error(t, err)
	_, err = stack.Pop()
	assert.Error(t, err)
	assert.Equal(t, 0, stack.Len())
}
