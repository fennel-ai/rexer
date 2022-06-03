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
