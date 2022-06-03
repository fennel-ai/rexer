package slice_test

import (
	"fennel/lib/utils/slice"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFill(t *testing.T) {
	arr := make([]int, 10)
	for i := range arr {
		arr[i] = i
	}
	for i := 0; i < len(arr); i++ {
		assert.Equal(t, i, arr[i])
	}
	// Now, fill the last 5 elements with -1
	slice.Fill(arr[len(arr)-5:], -1)
	for i := 0; i < len(arr)-6; i++ {
		assert.Equal(t, i, arr[i])
	}
	for i := len(arr) - 5; i < len(arr); i++ {
		assert.Equal(t, -1, arr[i])
	}
}
