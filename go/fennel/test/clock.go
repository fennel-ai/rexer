package test

import "fennel/lib/clock"

type FakeClock struct {
	now int64
}

func (f *FakeClock) Now() int64 {
	return f.now
}
func (f *FakeClock) Set(now int64) {
	f.now = now
}

var _ clock.Clock = &FakeClock{}
