package value

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewIntList(t *testing.T) {
	values := make([]Value, 0)
	values = append(values, Int(1))
	values = append(values, Int(2))
	list := NewList(values)
	assert.Equal(t, List(values), list)
}

func TestNewDict(t *testing.T) {
	values := make(map[string]Value, 0)
	values["a"] = Int(1)
	values["b"] = String("hi")
	ret, err := NewDict(values)
	assert.NoError(t, err)
	assert.Equal(t, Dict(map[string]Value{"a": Int(1), "b": String("hi")}), ret)
}

func TestList_Iter(t *testing.T) {
	row1, _ := NewDict(map[string]Value{
		"a": Int(1),
		"b": String("hi"),
	})
	row2, _ := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("bye"),
	})
	row3, _ := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("bye"),
	})
	row4, _ := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("fourt"),
	})
	table := List{}
	assert.Equal(t, 0, len(table))
	err := table.Append(row1)
	assert.Equal(t, 1, len(table))
	assert.NoError(t, err)
	err = table.Append(row2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(table))
	err = table.Append(row3)
	assert.NoError(t, err)
	assert.Equal(t, 3, len(table))

	// now create an iter object and iterate through it
	it := table.Iter()
	// before we do anything, now add another row to the table - this should never show in iterator
	err = table.Append(row4)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(table))

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
	l := NewList([]Value{})
	assert.Len(t, l, 0)

	assert.NoError(t, l.Append(Int(2)))
	assert.Len(t, l, 1)
	assert.NoError(t, l.Append(Bool(false)))
	assert.Len(t, l, 2)
	assert.Equal(t, List{Int(2), Bool(false)}, l)
}

func TestStringingNilValue(t *testing.T) {
	l1 := List{nil}
	l2 := List{List{nil}}
	d1 := Dict{"0": nil}
	d2 := Dict{"0": Dict{"_": nil}}

	assert.Equal(t, `[null]`, l1.String())
	assert.Equal(t, `[[null]]`, l2.String())
	assert.Equal(t, `{"0":null}`, d1.String())
	assert.Equal(t, `{"0":{"_":null}}`, d2.String())
}

func TestFuture_Await(t *testing.T) {
	v := Int(5)
	f := getFuture(v)
	assert.Equal(t, v.String(), f.String())
}
