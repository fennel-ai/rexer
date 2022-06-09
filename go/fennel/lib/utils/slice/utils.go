package slice

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

func Grow[T any](slice []T, n int) []T {
	if cap(slice) >= len(slice)+n {
		return slice
	}
	newSlice := make([]T, len(slice), len(slice)+n)
	copy(newSlice, slice)
	return newSlice
}
