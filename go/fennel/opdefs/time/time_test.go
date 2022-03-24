package time

import (
	"math/rand"
	"testing"

	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestDayOfWeek_Valid(t *testing.T) {
	t.Parallel()
	// operator doesn't need real tier, so creating a fake
	tier := tier.Tier{}
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
	var inputs []value.Dict
	var context []value.Dict
	var expected []value.Value
	name := utils.RandString(6)
	for i, case_ := range cases {
		n := rand.Uint32()
		context = append(context, value.NewDict(map[string]value.Value{"timestamp": value.Int(int64(n)*week + case_.timestamp)}))
		inputs = append(inputs, value.NewDict(map[string]value.Value{"something_else": value.Int(i)}))
		expected = append(expected, value.NewDict(map[string]value.Value{"something_else": value.Int(i), name: value.Int(case_.day)}))
	}
	optest.Assert(t, tier, op, value.NewDict(map[string]value.Value{"name": value.String(name)}), inputs, context, expected)
}

func TestDayOfWeek_Invalid(t *testing.T) {
	t.Parallel()
	// operator doesn't need real tier, so creating a fake
	tier := tier.Tier{}
	op := dayOfWeek{}
	week := int64(7 * 24 * 3600)
	cases := []int64{-1, -12312312, 0}
	name := utils.RandString(6)
	for _, case_ := range cases {
		inputs := make([]value.Dict, 0)
		context := make([]value.Dict, 0)
		context = append(context, value.NewDict(map[string]value.Value{"timestamp": value.Int(week)}), value.NewDict(map[string]value.Value{"timestamp": value.Int(case_)}))
		inputs = append(inputs, value.NewDict(map[string]value.Value{}), value.NewDict(map[string]value.Value{}))
		optest.AssertError(t, tier, op, value.NewDict(map[string]value.Value{"name": value.String(name)}), inputs, context)
	}
}

func TestTimeBucketOfDay_Valid(t *testing.T) {
	t.Parallel()
	// operator doesn't need real tier, so creating a fake
	tier := tier.Tier{}
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
		var inputs []value.Dict
		var context []value.Dict
		var expected []value.Value
		name := utils.RandString(6)
		n := rand.Uint32()
		for i, case_ := range scenario {
			context = append(context, value.NewDict(map[string]value.Value{"timestamp": value.Int(int64(n)*day + case_.timestamp)}))
			inputs = append(inputs, value.NewDict(map[string]value.Value{"something_else": value.Int(i)}))
			expected = append(expected, value.NewDict(map[string]value.Value{"something_else": value.Int(i), name: value.Int(case_.index)}))
		}
		optest.Assert(t, tier, op, value.NewDict(map[string]value.Value{"bucket": value.Int(bucket), "name": value.String(name)}), inputs, context, expected)
	}
}

func TestTimeBucketOfDay_Invalid(t *testing.T) {
	t.Parallel()
	// operator doesn't need real tier, so creating a fake
	tier := tier.Tier{}
	op := timeBucketOfDay{}
	name := value.String(utils.RandString(6))
	optest.AssertError(t, tier, op, value.NewDict(map[string]value.Value{"name": name, "bucket": value.Int(3600)}), []value.Dict{
		value.NewDict(map[string]value.Value{}),
		value.NewDict(map[string]value.Value{}),
	}, []value.Dict{
		value.NewDict(map[string]value.Value{"timestamp": value.Int(24 * 3600)}),
		value.NewDict(map[string]value.Value{"timestamp": value.Int(-1123)}),
	})
	optest.AssertError(t, tier, op,
		value.NewDict(map[string]value.Value{"name": name, "bucket": value.Int(3600)}), []value.Dict{
			value.NewDict(map[string]value.Value{}),
			value.NewDict(map[string]value.Value{}),
		}, []value.Dict{
			value.NewDict(map[string]value.Value{"timestamp": value.Int(24 * 3600)}),
			value.NewDict(map[string]value.Value{"timestamp": value.Int(-1)}),
		})
	optest.AssertError(t, tier, op,
		value.NewDict(map[string]value.Value{"name": name, "bucket": value.Int(3600)}), []value.Dict{
			value.NewDict(map[string]value.Value{}),
			value.NewDict(map[string]value.Value{}),
		}, []value.Dict{
			value.NewDict(map[string]value.Value{"timestamp": value.Int(24 * 3600)}),
			value.NewDict(map[string]value.Value{"timestamp": value.Int(0)}),
		})
	optest.AssertError(t, tier, op,
		value.NewDict(map[string]value.Value{"name": name, "bucket": value.Int(0)}),
		[]value.Dict{
			value.NewDict(map[string]value.Value{}),
			value.NewDict(map[string]value.Value{}),
		}, []value.Dict{
			value.NewDict(map[string]value.Value{"timestamp": value.Int(24 * 3600)}),
			value.NewDict(map[string]value.Value{"timestamp": value.Int(351)}),
		})
}
