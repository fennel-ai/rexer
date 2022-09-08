package gravel

import "github.com/AndreasBriese/bbloom"

type Bloom struct {
	filter bbloom.Bloom
}

func (b *Bloom) Has(key []byte) bool {
	return b.filter.Has(key)
}

func (b *Bloom) Add(key []byte) {
	b.filter.Add(key)
}

func (b *Bloom) Dump() []byte {
	return b.filter.JSONMarshal()
}

func NewBloomFilter(capacity uint64, fprate float64) Bloom {
	return Bloom{
		filter: bbloom.New(float64(capacity), fprate),
	}
}

func LoadBloom(data []byte) Bloom {
	return Bloom{
		filter: bbloom.JSONUnmarshal(data),
	}
}
