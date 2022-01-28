package clock

import "time"

type Clock interface {
	Now() int64
}

type Unix struct{}

var _ Clock = Unix{}

func (u Unix) Now() int64 {
	return time.Now().Unix()
}
