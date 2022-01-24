package value

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewIntList(t *testing.T) {
	values := make([]Value, 0)
	values = append(values, Int(1))
	values = append(values, Int(2))
	ret, err := NewList(values)
	list := ret.(List)
	assert.NoError(t, err)
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

func TestTableBasic(t *testing.T) {
	row1, _ := NewDict(map[string]Value{
		"a": Int(1),
		"b": String("hi"),
	})
	row2, _ := NewDict(map[string]Value{
		"a": Int(5),
		"b": String("bye"),
	})
	table := NewTable()
	assert.Equal(t, 0, table.Len())
	assert.Equal(t, 0, len(table.rows))
	err := table.Append(row1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(table.rows))
	assert.Equal(t, 1, table.Len())
	assert.Equal(t, []Dict{row1}, table.Pull())

	err = table.Append(row2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(table.rows))
	assert.Equal(t, 2, table.Len())
	assert.Equal(t, []Dict{row1, row2}, table.Pull())

	cloned := table.Clone()
	assert.True(t, table.Equal(cloned))
}

func TestTableSchema(t *testing.T) {
	row1, _ := NewDict(map[string]Value{
		"a": Int(1),
		"b": String("hi"),
	})
	// row 2 has different column names from row1
	row2, _ := NewDict(map[string]Value{
		"c": Int(5),
		"b": String("bye"),
	})
	// row 3 has same col names but different type
	row3, _ := NewDict(map[string]Value{
		"a": Double(1),
		"b": String("hi"),
	})
	table := NewTable()
	assert.Equal(t, 0, len(table.rows))
	err := table.Append(row1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(table.rows))
	assert.Equal(t, []Dict{row1}, table.Pull())

	err = table.Append(row2)
	assert.Error(t, err)
	assert.Equal(t, 1, len(table.rows))
	assert.Equal(t, []Dict{row1}, table.Pull())

	err = table.Append(row3)
	assert.Error(t, err)
	assert.Equal(t, 1, len(table.rows))
	assert.Equal(t, []Dict{row1}, table.Pull())

	cloned := table.Clone()
	assert.True(t, table.Equal(cloned))
}

func TestTableSchemaNested(t *testing.T) {
	row1, _ := NewDict(map[string]Value{
		"a.inner": Int(1),
		"b":       String("hi"),
	})
	inner, _ := NewDict(map[string]Value{"inner": Int(1)})

	row2, _ := NewDict(map[string]Value{
		"b": String("bye"),
		"a": inner,
	})
	row3, _ := NewDict(map[string]Value{
		"a.inner": Int(7),
		"b":       String("hello"),
	})
	table := NewTable()
	err := table.Append(row1)
	assert.NoError(t, err)
	err = table.Append(row2)
	assert.NoError(t, err)
	err = table.Append(row3)
	assert.NoError(t, err)

	assert.Equal(t, 3, len(table.rows))
	assert.Equal(t, []Dict{row1, row2.flatten(), row3}, table.Pull())
}

func TestTable_Iter(t *testing.T) {
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
	table := NewTable()
	assert.Equal(t, 0, table.Len())
	err := table.Append(row1)
	assert.Equal(t, 1, table.Len())
	assert.NoError(t, err)
	err = table.Append(row2)
	assert.NoError(t, err)
	assert.Equal(t, 2, table.Len())
	err = table.Pop()
	assert.NoError(t, err)
	assert.Equal(t, 1, table.Len())
	err = table.Append(row3)
	assert.NoError(t, err)
	assert.Equal(t, 2, table.Len())

	// now create an iter object and iterate through it
	it := table.Iter()
	// before we do anything, now add another row to the table - this should never show in iterator
	err = table.Append(row4)
	assert.NoError(t, err)
	assert.Equal(t, 3, table.Len())

	// okay now let's start asserting our iter
	assert.True(t, it.HasMore())
	found1, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, row1, found1)
	assert.True(t, it.HasMore())

	found2, err := it.Next()
	assert.NoError(t, err)
	assert.Equal(t, row3, found2)

	// now we can't iterate any more and if we try we get an error
	assert.False(t, it.HasMore())
	_, err = it.Next()
	assert.Error(t, err)
}
