package time

import (
	"testing"

	"fennel/lib/value"
	"fennel/test/optest"
	"fennel/tier"
)

func TestExtractor_Apply(t *testing.T) {
	t.Parallel()
	scenarios := []struct {
		inputs   []value.Value
		static   value.Dict
		kwargs   []value.Dict
		err      bool
		expected []value.Value
	}{
		{
			[]value.Value{
				value.Double(1655055404.0), // Sun Jun 12 2022 17:36:44 GMT+0000
				value.Int(1655055404 + 86400),
				value.Int(1655055404 + 86400*2),
				value.Int(1655055404 + 86400*3),
			},
			value.NewDict(map[string]value.Value{"element": value.String("weekday")}),
			[]value.Dict{{}, {}, {}, {}},
			false,
			[]value.Value{value.Int(0), value.Int(1), value.Int(2), value.Int(3)},
		},
		{
			[]value.Value{
				value.Double(1655055404.0), // Sun Jun 12 2022 17:36:44 GMT+0000
				value.Int(1655055404 + 86400),
				value.Int(1655055404 + 86400*2),
				value.Int(1655055404 + 86400*3),
			},
			value.NewDict(map[string]value.Value{"element": value.String("second")}),
			[]value.Dict{{}, {}, {}, {}},
			false,
			[]value.Value{value.Int(44), value.Int(44), value.Int(44), value.Int(44)},
		},
		{
			[]value.Value{
				value.Double(1655055404.0), // Sun Jun 12 2022 17:36:44 GMT+0000
				value.Int(1655055404 + 86400),
				value.Int(1655055404 + 86400*2),
				value.Int(1655055404 + 86400*3),
			},
			value.NewDict(map[string]value.Value{"element": value.String("typosecond")}),
			[]value.Dict{{}, {}, {}, {}},
			true,
			nil,
		},
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404)}),
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404 + 86400)}),
			},

			value.NewDict(map[string]value.Value{"element": value.String("weekday")}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"by": value.Int(1655055404)}),
				value.NewDict(map[string]value.Value{"by": value.Int(1655055404 + 86400)}),
			},
			false,
			[]value.Value{value.Int(0), value.Int(1)},
		},
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404)}),
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404 + 86400)}),
			},

			value.NewDict(map[string]value.Value{"element": value.String("weekday"), "field": value.String("weekdayfield")}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"by": value.Int(1655055404)}),
				value.NewDict(map[string]value.Value{"by": value.Int(1655055404 + 86400)}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404), "weekdayfield": value.Int(0)}),
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404 + 86400), "weekdayfield": value.Int(1)}),
			},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, Extractor{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			optest.AssertEqual(t, tier.Tier{}, Extractor{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expected)
		}
	}
}
