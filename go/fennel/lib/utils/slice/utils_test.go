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

func TestGrow(t *testing.T) {
	arr := make([]int, 10)
	for i := range arr {
		arr[i] = i
	}
	for i := 0; i < len(arr); i++ {
		assert.Equal(t, i, arr[i])
	}
	// Both length and capacity are 10.
	assert.Equal(t, 10, len(arr))
	assert.Equal(t, len(arr), cap(arr))
	// Now, grow the slice by 5 elements
	arr = slice.Grow(arr, 5)
	// Length should still be 10, but capacity should now be 15.
	assert.Equal(t, 10, len(arr))
	assert.Equal(t, len(arr)+5, cap(arr))
	for i := 0; i < len(arr); i++ {
		assert.Equal(t, i, arr[i])
	}
}

func TestLimit(t *testing.T) {
	arr := make([]int, 10)
	for i := range arr {
		arr[i] = i
	}
	for i := 0; i < len(arr); i++ {
		assert.Equal(t, i, arr[i])
	}
	// Now, create limited view to a part of the slice.
	part := arr[0:5]
	limited := slice.Limit(part)
	assert.Equal(t, 5, len(limited))
	assert.Equal(t, 5, cap(limited))
	// Modifications to the base slice are reflected in the view if it has not
	// been appended to.
	part[0] = -1
	assert.Equal(t, -1, limited[0])
	// However, appending to the limited view creates a copy.
	limited = append(limited, -2)
	assert.Equal(t, 6, len(limited))
	assert.Equal(t, -2, limited[5])
	assert.NotEqual(t, -2, arr[5])
	// Now, modifications to original slice are also not reflected in the
	// limited view.
	part[2] = 16
	assert.NotEqual(t, 16, limited[2])
}
