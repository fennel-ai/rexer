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
	table.Append(row1)
	assert.Equal(t, 1, table.Len())
	table.Append(row2)
	assert.Equal(t, 2, table.Len())
	table.Append(row3)
	assert.Equal(t, 3, table.Len())

	// now create an iter object and iterate through it
	it := table.Iter()
	// before we do anything, now add another row to the table - this should never show in iterator
	table.Append(row4)
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

	l.Append(Int(2))
	assert.Equal(t, 1, l.Len())
	l.Append(Bool(false))
	assert.Equal(t, 2, l.Len())
	assert.Equal(t, NewList(Int(2), Bool(false)), l)
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
