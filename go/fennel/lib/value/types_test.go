package value

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewIntList(t *testing.T) {
	values := make([]Value, 0)
	values = append(values, Int(1))
	values = append(values, Int(2))
	list := NewList(values...)
	assert.Equal(t, NewList(values...), list)
}

func TestNewDict(t *testing.T) {
	values := make(map[string]Value)
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

func TestStringingNilValue(t *testing.T) {
	l1 := List{nil}
	l2 := List{[]Value{List{}}}
	d1 := Dict{map[string]Value{"0": nil}}

	d2 := Dict{map[string]Value{"0": Dict{map[string]Value{"_": nil}}}}

	assert.Equal(t, `[]`, l1.String())
	assert.Equal(t, `[[]]`, l2.String())
	assert.Equal(t, `{"0":null}`, d1.String())
	assert.Equal(t, `{"0":{"_":null}}`, d2.String())
}

// Test that calling Grow() on a list before appending can save allocations.
func TestListGrow(t *testing.T) {
	l := NewList()
	v1 := l.Values()
	// Insert elements without growing first.
	l.Append(Int(1), Int(2))
	v2 := l.Values()
	assert.True(t, (*reflect.SliceHeader)(unsafe.Pointer(&v1)).Data != (*reflect.SliceHeader)(unsafe.Pointer(&v2)).Data)

	l = NewList()
	// Growing the list should not allocate.
	l.Grow(2)
	require.Equal(t, 0, l.Len())
	v1 = l.Values()
	l.Append(Int(1), Int(2))
	require.Equal(t, 2, l.Len())
	v2 = l.Values()
	assert.True(t, (*reflect.SliceHeader)(unsafe.Pointer(&v1)).Data == (*reflect.SliceHeader)(unsafe.Pointer(&v2)).Data)
	// Inserting without allocating more capacity will still allocate.
	l.Append(Int(1), Int(2))
	v3 := l.Values()
	assert.True(t, (*reflect.SliceHeader)(unsafe.Pointer(&v3)).Data != (*reflect.SliceHeader)(unsafe.Pointer(&v2)).Data)
	assert.Equal(t, 4, l.Len())
}
