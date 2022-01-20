package operators

import (
	"fennel/lib/value"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getTable() value.Table {
	row1, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(1),
		"b":       value.String("hi"),
	})
	inner, _ := value.NewDict(map[string]value.Value{"inner": value.Int(1)})

	row2, _ := value.NewDict(map[string]value.Value{
		"b": value.String("bye"),
		"a": inner,
	})
	row3, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(7),
		"b":       value.String("hello"),
	})
	table := value.NewTable()
	table.Append(row1)
	table.Append(row2)
	table.Append(row3)
	return table
}

func testValid(t *testing.T, op Operator, kwargs value.Dict, intable value.Table, expected value.Table) {
	outtable := value.NewTable()
	err := op.Apply(kwargs, intable, &outtable)
	assert.NoError(t, err)
	assert.Equal(t, expected, outtable)
}

func TestFilterOperator_Apply(t *testing.T) {
	op, err := Locate("std", "filter")
	assert.NoError(t, err)

	intable := getTable()
	// not passing "where" fails Validation
	assert.Error(t, Validate(op, value.Dict{}, map[string]reflect.Type{}))

	// passing where true works
	kwargs := value.Dict{"where": value.Bool(true)}
	assert.NoError(t, Validate(op, kwargs, map[string]reflect.Type{}))
	testValid(t, op, kwargs, intable, intable)

	// and when we filter everything, we should get empty table
	kwargs = value.Dict{"where": value.Bool(false)}
	assert.NoError(t, Validate(op, kwargs, map[string]reflect.Type{}))
	testValid(t, op, kwargs, intable, value.NewTable())
}

func TestTakeOperator_Apply(t *testing.T) {
	op, err := Locate("std", "take")
	assert.NoError(t, err)

	intable := getTable()
	// not passing "limit" fails validation
	assert.Error(t, Validate(op, value.Dict{}, map[string]reflect.Type{}))

	// and it fails validation even when limit is passed but isn't int
	assert.Error(t, Validate(op, value.Dict{"limit": value.Bool(true)}, map[string]reflect.Type{}))

	// passing limit 2 works
	expected := value.NewTable()
	for i, row := range intable.Pull() {
		if i < 2 {
			expected.Append(row)
		}
	}
	testValid(t, op, value.Dict{"limit": value.Int(2)}, intable, expected)

	// and when the limit is very large, it only returns intable as it is
	testValid(t, op, value.Dict{"limit": value.Int(10000)}, intable, intable)
}
