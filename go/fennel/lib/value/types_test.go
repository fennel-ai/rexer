package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIntList(t *testing.T) {
	values := make([]Value, 0)
	values = append(values, Int(1))
	values = append(values, Int(2))
	list := NewList(values...)
	assert.Equal(t, NewList(values...), list)
}

func TestNewDict(t *testing.T) {
	values := make(map[string]Value, 0)
	values["a"] = Int(1)
	values["b"] = String("hi")
	ret := NewDict(values)
	assert.Equal(t, NewDict(map[string]Value{"a": Int(1), "b": String("hi")}), ret)
}

func TestList_Iter(t *testing.T) {
	row1 := NewDict(map[string]Value{
		"a": Int(1),
		"b": String("hi"),
	})
	row2 := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("bye"),
	})
	row3 := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("bye"),
	})
	row4 := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("fourt"),
	})
	table := List{}
	assert.Equal(t, 0, table.Len())
	err := table.Append(row1)
	assert.Equal(t, 1, table.Len())
	assert.NoError(t, err)
	err = table.Append(row2)
	assert.NoError(t, err)
	assert.Equal(t, 2, table.Len())
	err = table.Append(row3)
	assert.NoError(t, err)
	assert.Equal(t, 3, table.Len())

	// now create an iter object and iterate through it
	it := table.Iter()
	// before we do anything, now add another row to the table - this should never show in iterator
	err = table.Append(row4)
	assert.NoError(t, err)
	assert.Equal(t, 4, table.Len())

	// okay now let's start asserting our iter
	assert.True(t, it.HasMore())
	found1, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, row1, found1)
	assert.True(t, it.HasMore())

	found2, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, row2, found2)

	found3, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, row3, found3)

	// now we can't iterate any more and if we try we get an error
	assert.False(t, it.HasMore())
	_, err = it.Next()
	assert.Error(t, err)
}

func TestList_Append(t *testing.T) {
	t.Parallel()
	l := NewList()
	assert.Equal(t, 0, l.Len())

	assert.NoError(t, l.Append(Int(2)))
	assert.Equal(t, 1, l.Len())
	assert.NoError(t, l.Append(Bool(false)))
	assert.Equal(t, 2, l.Len())
	assert.Equal(t, NewList(Int(2), Bool(false)), l)

	// no nested lists allowed
	assert.NoError(t, l.Append(NewList(Double(3.0), Bool(false), Nil)))
	assert.Equal(t, NewList(Int(2), Bool(false), Double(3.0), Bool(false), Nil), l)

	// and this works even when we do multiple level nesting
	assert.NoError(t, l.Append(NewList(NewList(NewList(String("hi"))))))
	assert.Equal(t, NewList(Int(2), Bool(false), Double(3.0), Bool(false), Nil, String("hi")), l)
}

//func TestStringingNilValue(t *testing.T) {
//	l1 := List{nil}
//	l2 := List{List{nil}}
//	d1 := Dict{"0": nil}
//	d2 := Dict{"0": Dict{"_": nil}}
//
//	assert.Equal(t, `[null]`, l1.String())
//	assert.Equal(t, `[[null]]`, l2.String())
//	assert.Equal(t, `{"0":null}`, d1.String())
//	assert.Equal(t, `{"0":{"_":null}}`, d2.String())
//}

func TestFuture_Await(t *testing.T) {
	v := Int(5)
	f := getFuture(v)
	assert.Equal(t, v.String(), f.String())
}

func Test_Unwrap(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		v   Value
		e   Value
		err bool
	}{
		{Int(2), Int(2), false},
		{Double(2.0), Double(2.0), false},
		{Nil, Nil, false},
		{String("hi"), String("hi"), false},
		{NewDict(map[string]Value{"x": Int(1), "y": NewList()}), NewDict(map[string]Value{"x": Int(1), "y": NewList()}), false},
		{Bool(true), Bool(true), false},
		{NewList(Int(3)), Int(3), false},
		{List{}, nil, true},
		{NewList(Int(2), Nil), nil, true},
	}
	for _, scene := range scenarios {
		found, err := scene.v.Unwrap()
		if scene.err {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
			assert.Equal(t, scene.e, found)
		}
	}
}

func Test_Wrap(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		v Value
		e Value
	}{
		{Int(2), NewList(Int(2))},
		{Double(2.0), NewList(Double(2.0))},
		{Nil, NewList(Nil)},
		{String("hi"), NewList(String("hi"))},
		{NewDict(map[string]Value{"x": Int(1), "y": List{}}), NewList(NewDict(map[string]Value{"x": Int(1), "y": List{}}))},
		{Bool(true), NewList(Bool(true))},
		{NewList(Int(3)), NewList(Int(3))},
		{List{}, List{}},
		{NewList(Int(2), Nil), NewList(Int(2), Nil)},
	}
	for _, scene := range scenarios {
		found := scene.v.Wrap()
		assert.Equal(t, scene.e, found)
	}
}
