package opdefs

import (
	"fennel/engine/operators"
	"fennel/lib/utils"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
)

func check(t *testing.T, op operators.Operator, static value.Dict, inputs, context []value.Dict, expected []value.Dict) {
	iter := utils.NewZipTable()
	for i, row := range inputs {
		assert.NoError(t, iter.Append(row, context[i]))
	}
	outtable := value.NewTable()
	assert.NoError(t, op.Apply(static, iter.Iter(), &outtable))
	found := outtable.Pull()
	assert.Len(t, found, len(expected))
	assert.ElementsMatch(t, expected, found)
}

func checkerror(t *testing.T, op operators.Operator, static value.Dict, inputs, context []value.Dict) {
	iter := utils.NewZipTable()
	for i, row := range inputs {
		assert.NoError(t, iter.Append(row, context[i]))
	}
	outtable := value.NewTable()
	assert.Error(t, op.Apply(static, iter.Iter(), &outtable))
}

func TestDayOfWeek_Valid(t *testing.T) {
	t.Parallel()
	op := dayOfWeek{}
	week := int64(7 * 24 * 3600)
	day := int64(24 * 3600)
	cases := []struct {
		timestamp int64
		day       int64
	}{
		{1, 0}, {day - 1, 0},
		{day, 1}, {day + 1, 1}, {2*day - 1, 1},
		{2 * day, 2}, {3*day - 1, 2},
		{3 * day, 3}, {4*day - 1, 3},
		{4 * day, 4}, {5*day - 1, 4},
		{5 * day, 5}, {6*day - 1, 5},
		{6 * day, 6}, {7*day - 1, 6},
	}
	inputs := make([]value.Dict, 0)
	context := make([]value.Dict, 0)
	expected := make([]value.Dict, 0)
	name := utils.RandString(6)
	for i, case_ := range cases {
		n := rand.Uint32()
		context = append(context, value.Dict{"timestamp": value.Int(int64(n)*week + case_.timestamp)})
		inputs = append(inputs, value.Dict{"something_else": value.Int(i)})
		expected = append(expected, value.Dict{"something_else": value.Int(i), name: value.Int(case_.day)})
	}
	check(t, op, value.Dict{"name": value.String(name)}, inputs, context, expected)
}

func TestDayOfWeek_Invalid(t *testing.T) {
	t.Parallel()
	op := dayOfWeek{}
	week := int64(7 * 24 * 3600)
	cases := []int64{-1, -12312312, 0}
	name := utils.RandString(6)
	for _, case_ := range cases {
		inputs := make([]value.Dict, 0)
		context := make([]value.Dict, 0)
		context = append(context, value.Dict{"timestamp": value.Int(week)}, value.Dict{"timestamp": value.Int(case_)})
		inputs = append(inputs, value.Dict{}, value.Dict{})
		checkerror(t, op, value.Dict{"name": value.String(name)}, inputs, context)
	}
}

func TestTimeBucketOfDay_Valid(t *testing.T) {
	t.Parallel()
	op := timeBucketOfDay{}
	day := int64(24 * 3600)
	cases := map[int64][]struct {
		timestamp int64
		index     int64
	}{
		3600:     {{1, 0}, {day - 1, 23}, {3600*9 + 5, 9}},
		3 * 3600: {{1, 0}, {day - 1, 7}, {3600*9 + 5, 3}},
		6 * 3600: {{1, 0}, {day - 1, 3}, {3600*9 + 5, 1}},
	}
	for bucket, scenario := range cases {
		inputs := make([]value.Dict, 0)
		context := make([]value.Dict, 0)
		expected := make([]value.Dict, 0)
		name := utils.RandString(6)
		n := rand.Uint32()
		for i, case_ := range scenario {
			context = append(context, value.Dict{"timestamp": value.Int(int64(n)*day + case_.timestamp)})
			inputs = append(inputs, value.Dict{"something_else": value.Int(i)})
			expected = append(expected, value.Dict{"something_else": value.Int(i), name: value.Int(case_.index)})
		}
		check(t, op, value.Dict{"bucket": value.Int(bucket), "name": value.String(name)}, inputs, context, expected)
	}
}

func TestTimeBucketOfDay_Invalid(t *testing.T) {
	t.Parallel()
	op := timeBucketOfDay{}
	name := value.String(utils.RandString(6))
	checkerror(t, op, value.Dict{"name": name, "bucket": value.Int(3600)}, []value.Dict{{}, {}}, []value.Dict{
		{"timestamp": value.Int(24 * 3600)}, {"timestamp": value.Int(-1123)},
	})
	checkerror(t, op, value.Dict{"name": name, "bucket": value.Int(3600)}, []value.Dict{{}, {}}, []value.Dict{
		{"timestamp": value.Int(24 * 3600)}, {"timestamp": value.Int(-1)},
	})
	checkerror(t, op, value.Dict{"name": name, "bucket": value.Int(3600)}, []value.Dict{{}, {}}, []value.Dict{
		{"timestamp": value.Int(24 * 3600)}, {"timestamp": value.Int(0)},
	})
	checkerror(t, op, value.Dict{"name": name, "bucket": value.Int(0)}, []value.Dict{{}, {}}, []value.Dict{
		{"timestamp": value.Int(24 * 3600)}, {"timestamp": value.Int(351)},
	})
}
