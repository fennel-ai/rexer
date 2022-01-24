package utils

import (
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewZipTable(t *testing.T) {
	zt := NewZipTable()
	assert.Equal(t, 0, zt.Len())
	row1, _ := value.NewDict(map[string]value.Value{
		"a": value.Int(1),
		"b": value.String("hi"),
	})
	row2, _ := value.NewDict(map[string]value.Value{
		"a": value.Int(5),
		"b": value.String("bye"),
	})
	row3, _ := value.NewDict(map[string]value.Value{
		"a": value.Int(9),
		"b": value.String("third"),
	})
	row4, _ := value.NewDict(map[string]value.Value{
		"a": value.Int(122),
		"b": value.String("fourt"),
	})
	badrow, _ := value.NewDict(map[string]value.Value{
		"x": value.Int(122),
		"y": value.String("fourt"),
	})
	err := zt.Append(row1, row2)
	assert.NoError(t, err)
	assert.Equal(t, 1, zt.Len())
	err = zt.Append(row3, row4)
	assert.NoError(t, err)
	assert.Equal(t, 2, zt.Len())

	// and if any of the two throw append error, so does the whole thing
	err = zt.Append(badrow, row2)
	assert.Error(t, err)
	assert.Equal(t, 2, zt.Len())
	err = zt.Append(row1, badrow)
	assert.Error(t, err)
	assert.Equal(t, 2, zt.Len())

	// now create an iterate, which won't be affected by original ziptable getting more entries
	iter := zt.Iter()
	err = zt.Append(row2, row3)
	assert.NoError(t, err)
	assert.Equal(t, 3, zt.Len())

	assert.True(t, iter.HasMore())
	found1, found2, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, row1, found1)
	assert.Equal(t, row2, found2)

	assert.True(t, iter.HasMore())
	found3, found4, err := iter.Next()
	assert.NoError(t, err)
	assert.Equal(t, row3, found3)
	assert.Equal(t, row4, found4)

	assert.False(t, iter.HasMore())
	_, _, err = iter.Next()
	assert.Error(t, err)
}
