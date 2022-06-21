package time

import (
	"fmt"
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
				value.Double(1655055404.0), // Sun Jun 12 2022 17:36:44 GMT+0000, 10:36 Pacific time
				value.Int(1655055404 + 86400),
				value.Int(1655055404 + 86400*2),
				value.Int(1655055404 + 86400*3),
				value.Int(1655055404 + 86400*4),
			},
			value.Dict{},
			[]value.Dict{
				value.NewDict(map[string]value.Value{"part": value.String("hour"), "timezone": value.Double(-7.0)}),
				value.NewDict(map[string]value.Value{"part": value.String("hour"), "timezone": value.String("UTC")}),
				value.NewDict(map[string]value.Value{"part": value.String("hour"), "timezone": value.String("-07:00")}),
				value.NewDict(map[string]value.Value{"part": value.String("dayofweek"), "timezone": value.String("UTC")}),
				value.NewDict(map[string]value.Value{"part": value.String("rfc3339"), "timezone": value.String("+08:00")}),
			},
			false,
			[]value.Value{value.Int(10), value.Int(17), value.Int(10), value.Int(3), value.String("2022-06-17T01:36:44+08:00")},
		},
		{
			[]value.Value{
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404)}),
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404 + 86400)}),
				value.NewDict(map[string]value.Value{"myfield": value.String("2022-06-16T01:36:44+08:00")}),
				value.NewDict(map[string]value.Value{"myfield": value.String("2022-06-17T01:36:44+08:00")}),
			},
			value.NewDict(map[string]value.Value{"field": value.String("outputfield")}),
			[]value.Dict{
				value.NewDict(map[string]value.Value{"on": value.Int(1655055404), "part": value.String("dayofweek"), "timezone": value.String("+00:00")}),
				value.NewDict(map[string]value.Value{"on": value.Int(1655055404 + 86400), "part": value.String("dayofweek"), "timezone": value.String("UTC")}),
				value.NewDict(map[string]value.Value{"on": value.String("2022-06-16T01:36:44+08:00"), "part": value.String("timestamp"), "timezone": value.Double(7.5)}),
				value.NewDict(map[string]value.Value{"on": value.String("2022-06-17T01:36:44+08:00"), "part": value.String("rfc3339"), "timezone": value.String("America/Los_Angeles")}),
			},
			false,
			[]value.Value{
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404), "outputfield": value.Int(0)}),
				value.NewDict(map[string]value.Value{"myfield": value.Int(1655055404 + 86400), "outputfield": value.Int(1)}),
				value.NewDict(map[string]value.Value{"myfield": value.String("2022-06-16T01:36:44+08:00"), "outputfield": value.Int(1655055404 + 86400*3)}),
				value.NewDict(map[string]value.Value{"myfield": value.String("2022-06-17T01:36:44+08:00"), "outputfield": value.String("2022-06-16T10:36:44-07:00")}),
			},
		},
	}
	for _, scene := range scenarios {
		if scene.err {
			optest.AssertError(t, tier.Tier{}, Extractor{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs)
		} else {
			fmt.Println(scene.kwargs)
			optest.AssertEqual(t, tier.Tier{}, Extractor{}, scene.static, [][]value.Value{scene.inputs}, scene.kwargs, scene.expected)
		}
	}
}
