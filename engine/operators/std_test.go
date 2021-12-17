package operators

import (
	"engine/runtime"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func getTable() runtime.Table {
	row1, _ := runtime.NewDict(map[string]runtime.Value{
		"a.inner": runtime.Int(1),
		"b":       runtime.String("hi"),
	})
	inner, _ := runtime.NewDict(map[string]runtime.Value{"inner": runtime.Int(1)})

	row2, _ := runtime.NewDict(map[string]runtime.Value{
		"b": runtime.String("bye"),
		"a": inner,
	})
	row3, _ := runtime.NewDict(map[string]runtime.Value{
		"a.inner": runtime.Int(7),
		"b":       runtime.String("hello"),
	})
	table := runtime.NewTable()
	table.Append(row1)
	table.Append(row2)
	table.Append(row3)
	return table
}

func testValid(t *testing.T, op Operator, kwargs runtime.Dict, intable runtime.Table, expected runtime.Table) {
	outtable := runtime.NewTable()
	err := op.Apply(kwargs, intable, &outtable)
	assert.NoError(t, err)
	assert.Equal(t, expected, outtable)
}

func TestFilterOperator_Apply(t *testing.T) {
	op, err := Locate("std", "filter")
	assert.NoError(t, err)

	intable := getTable()
	// not passing "where" fails Validation
	assert.Error(t, Validate(op, runtime.Dict{}, map[string]reflect.Type{}))

	// passing where true works
	kwargs := runtime.Dict{"where": runtime.Bool(true)}
	assert.NoError(t, Validate(op, kwargs, map[string]reflect.Type{}))
	testValid(t, op, kwargs, intable, intable)

	// and when we filter everything, we should get empty table
	kwargs = runtime.Dict{"where": runtime.Bool(false)}
	assert.NoError(t, Validate(op, kwargs, map[string]reflect.Type{}))
	testValid(t, op, kwargs, intable, runtime.NewTable())
}

func TestTakeOperator_Apply(t *testing.T) {
	op, err := Locate("std", "take")
	assert.NoError(t, err)

	intable := getTable()
	// not passing "limit" fails validation
	assert.Error(t, Validate(op, runtime.Dict{}, map[string]reflect.Type{}))

	// and it fails validation even when limit is passed but isn't int
	assert.Error(t, Validate(op, runtime.Dict{"limit": runtime.Bool(true)}, map[string]reflect.Type{}))

	// passing limit 2 works
	expected := runtime.NewTable()
	for i, row := range intable.Pull() {
		if i < 2 {
			expected.Append(row)
		}
	}
	testValid(t, op, runtime.Dict{"limit": runtime.Int(2)}, intable, expected)

	// and when the limit is very large, it only returns intable as it is
	testValid(t, op, runtime.Dict{"limit": runtime.Int(10000)}, intable, intable)
}
