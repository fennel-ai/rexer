package stack

import (
	"errors"
	"fennel/lib/utils/slice"
)

var (
	Underflow = errors.New("stack underflow error")
)

type Stack[T any] struct {
	data []T
	top  int
	len_ int
}

// Push pushes an object onto the stack
// NOTE: this function code is carefully chosen to be simple enough that
// go compiler inlines it. If you modify it, please make sure it can still
// be inlined
func (s *Stack[T]) Push(obj T) {
	top := s.top
	if top >= s.len_ {
		s.data = slice.Grow(s.data, s.len_)
		s.data = s.data[:cap(s.data)]
		s.len_ = len(s.data)
	}
	s.data[top] = obj
	s.top = top + 1
}

// Pop pops the top element from the stack or returns an Underflow error if there is None
// NOTE: this function code is carefully chosen to be simple enough that
// go compiler inlines it. If you modify it, please make sure it can still
// be inlined
func (s *Stack[T]) Pop() (T, error) {
	top := s.top
	if top == 0 {
		var zero T
		return zero, Underflow
	}
	top -= 1
	s.top = top
	return s.data[top], nil
}

// Top returns the top element of the stack (without popping) or returns
// an Underflow error if there is none.
// NOTE: this function code is carefully chosen to be simple enough that
// go compiler inlines it. If you modify it, please make sure it can still
// be inlined
func (s *Stack[T]) Top() (T, error) {
	top := s.top
	var zero T
	if top == 0 {
		return zero, Underflow
	}
	return s.data[top-1], nil
}

// Len returns the number of elements in the stack
func (s *Stack[T]) Len() int {
	return s.top
}

// New returns a new stack of type T of given initial size
func New[T any](size int) Stack[T] {
	data := make([]T, size)
	return Stack[T]{
		data: data,
		top:  0,
		len_: size,
	}
}
