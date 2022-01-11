package main

import (
	"math"
)

const Z_95 = 1.96

func Wilson(num uint64, den uint64, lower bool) float64 {
	// (p + Z_95²/2n ± Z_95√p(1 – p)/n + Z_95²/4n²) / (1 + Z_95²/n)
	if den == 0 {
		return 0
	}

	p := float64(num) / float64(den)
	n := float64(den)
	base := p + (Z_95*Z_95)/(2*n)
	plusminus := Z_95 * math.Sqrt(p*(1-p)/n+(Z_95*Z_95)/(4*n*n))
	normalize := 1 + (Z_95*Z_95)/n
	if lower {
		return (base - plusminus) / normalize
	} else {
		return (base + plusminus) / normalize
	}
}
