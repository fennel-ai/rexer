package gravel

import "errors"

var (
	ErrNotFound = errors.New("key not found")
)

const (
	SUFFIX = ".grvl"
)

type Timestamp uint32

type Value struct {
	data    []byte
	expires Timestamp
	deleted bool
}

type Entry struct {
	key []byte
	val Value
}
