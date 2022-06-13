package slice

// Fill efficiently fills a slice to its length with the given value.
func Fill[T any](slice []T, elem T) {
	l := len(slice)
	if l == 0 {
		return
	}
	slice[0] = elem
	for j := 1; j < l; j *= 2 {
		copy(slice[j:], slice[:j])
	}
}

// Grow ensures the slice has the capacity to fit an additional n elements.
func Grow[T any](slice []T, n int) []T {
	if cap(slice) >= len(slice)+n {
		return slice
	}
	newSlice := make([]T, len(slice), len(slice)+n)
	copy(newSlice, slice)
	return newSlice
}

// Limit returns a full subslice of the given slice, but ensures that the
// capacity of the subslice is the same as its length. This ensures that the
// subslice is copied on append instead of modifying the underlying array.
// This is useful in cases where a large slice is broken into many smaller
// slices and the smaller slices are consecutively laid out in memory.
func Limit[T any](slice []T) []T {
	l := len(slice)
	return slice[:l:l]
}
