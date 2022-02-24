package std

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"fennel/engine/operators"
	"fennel/lib/utils"
	"fennel/lib/value"
)

func getTable() value.Table {
	row1, _ := value.NewDict(map[string]value.Value{
		"a.inner": value.Int(1),
		"b":       value.String("hi"),
	})
	row2, _ := value.NewDict(map[string]value.Value{
		"b":       value.String("bye"),
		"a.inner": value.Int(1),
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

func testValid(t *testing.T, op operators.Operator, staticKwargs value.Dict, intable value.Table, contextKwargTable value.Table, expected value.Table) {
	outtable := value.NewTable()
	zt := utils.NewZipTable()
	for i, in := range intable.Pull() {
		zt.Append(in, contextKwargTable.Pull()[i])
	}
	err := op.Apply(staticKwargs, zt.Iter(), &outtable)
	assert.NoError(t, err)
	assert.Equal(t, expected, outtable)
}

func TestFilterOperator_Apply(t *testing.T) {
	t.Parallel()
	op, err := operators.Locate("std", "filter")
	assert.NoError(t, err)

	intable := getTable()
	// not passing "where" fails Validation
	assert.Error(t, operators.Typecheck(op, map[string]reflect.Type{}, value.Types.Any, map[string]reflect.Type{}))

	// passing where true works
	whereTrue := value.Dict{"where": value.Bool(true)}
	whereFalse := value.Dict{"where": value.Bool(false)}
	assert.NoError(t, operators.Typecheck(op, map[string]reflect.Type{}, value.Types.Any, whereTrue.Schema()))

	contextKwargTable := value.NewTable()
	contextKwargTable.Append(whereTrue)
	contextKwargTable.Append(whereFalse)
	contextKwargTable.Append(whereTrue)
	expected := value.NewTable()
	expected.Append(intable.Pull()[0])
	expected.Append(intable.Pull()[2])
	testValid(t, op, whereTrue, intable, contextKwargTable, expected)

	// and when we filter everything, we should get empty table
	contextKwargTable = value.NewTable()
	contextKwargTable.Append(whereFalse)
	contextKwargTable.Append(whereFalse)
	contextKwargTable.Append(whereFalse)
	testValid(t, op, whereTrue, intable, contextKwargTable, value.NewTable())
}

func TestTakeOperator_Apply(t *testing.T) {
	t.Parallel()
	op, err := operators.Locate("std", "take")
	assert.NoError(t, err)

	intable := getTable()
	// not passing "limit" fails validation
	assert.Error(t, operators.Typecheck(op, map[string]reflect.Type{}, value.Types.Any, map[string]reflect.Type{}))

	// and it fails validation even when limit is passed but isn't int
	assert.Error(t, operators.Typecheck(op, map[string]reflect.Type{"limit": value.Types.Bool}, value.Types.Any, map[string]reflect.Type{}))

	// passing limit 2 works
	expected := value.NewTable()
	for i, row := range intable.Pull() {
		if i < 2 {
			expected.Append(row)
		}
	}
	contextKwargTable := value.NewTable()
	contextKwargTable.Append(value.Dict{})
	contextKwargTable.Append(value.Dict{})
	contextKwargTable.Append(value.Dict{})
	testValid(t, op, value.Dict{"limit": value.Int(2)}, intable, contextKwargTable, expected)

	// and when the limit is very large, it only returns intable as it is
	testValid(t, op, value.Dict{"limit": value.Int(10000)}, intable, contextKwargTable, intable)
}
