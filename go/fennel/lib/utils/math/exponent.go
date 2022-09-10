package math

func NextPowerOf2(n uint64) uint64 {
	if n > 0 && (n&(n-1) == 0) {
		return n
	}
	p := uint64(1)
	for p < n {
		p = p << 1
	}
	return p
}
