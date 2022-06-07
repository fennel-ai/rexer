package test

import "fennel/lib/clock"

type FakeClock struct {
	now uint32
}

func (f *FakeClock) Now() uint32 {
	return f.now
}
func (f *FakeClock) Set(now uint32) {
	f.now = now
}

var _ clock.Clock = &FakeClock{}
