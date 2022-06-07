package clock

import "time"

type Clock interface {
	// TODO(REX-1157): Move back to uint64
	Now() uint32
}

type Unix struct{}

var _ Clock = Unix{}

func (u Unix) Now() uint32 {
	return uint32(time.Now().Unix())
}
