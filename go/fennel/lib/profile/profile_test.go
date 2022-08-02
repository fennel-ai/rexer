package profile

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"

	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestProfileItemJSON(t *testing.T) {
	tests := []struct {
		str string
		pi  ProfileItem
	}{{
		str: `{"OType":"","Oid":"","Key":"","Value":null,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Nil},
	}, {
		str: `{"OType":"type1","Oid":"2","Key":"abc","Value":"xyz","UpdateTime":4}`,
		pi:  ProfileItem{OType: "type1", Oid: "2", Key: "abc", UpdateTime: 4, Value: value.String("xyz")},
	}, {
		str: `{"OType":"","Oid":"","Key":"","Value":false,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Bool(false)},
	}, {
		str: `{"OType":"","Oid":"","Key":"","Value":5,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Int(5)},
	}, {
		str: `{"OType":"","Oid":"","Key":"","Value":3.14,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Double(3.14)},
	}, {
		str: `{"OType":"","Oid":"","Key":"","Value":[],"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.NewList()},
	},
		{
			str: `{"OType":"","Oid":"","Key":"","Value":[[]],"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewList(value.NewList())},
		},
		{
			str: `{"OType":"","Oid":"","Key":"","Value":[null],"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewList(value.Nil)},
		}, {
			str: `{"OType":"","Oid":"","Key":"","Value":{},"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewDict(nil)},
		}, {
			str: `{"OType":"","Oid":"","Key":"","Value":{"0":{}},"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewDict(map[string]value.Value{"0": value.NewDict(nil)})},
		}, {
			str: `{"OType":"","Oid":"","Key":"","Value":{"k1":4.5},"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewDict(map[string]value.Value{"k1": value.Double(4.5)})},
		}, {
			str: fmt.Sprintf(`{"OType":"","Oid":"","Key":"","Value":null,"UpdateTime":%d}`, uint64(math.MaxUint64)),
			pi:  ProfileItem{UpdateTime: math.MaxUint64, Value: value.Nil},
		}}
	// Test unmarshal
	for _, tst := range tests {
		var pi ProfileItem
		err := json.Unmarshal([]byte(tst.str), &pi)
		assert.NoError(t, err)
		// assert.Equal(t, tst.pi, pi) does not work
		assert.True(t, tst.pi.Equals(&pi))
	}
	// Test marshal
	for _, tst := range tests {
		ser, err := json.Marshal(tst.pi)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(ser))
	}
}

func TestProfileFromValueDict(t *testing.T) {
	tests := []struct {
		v value.Dict
		p ProfileItem
	}{{
		v: value.NewDict(map[string]value.Value{
			"oid":       value.Int(1),
			"otype":     value.String("user"),
			"timestamp": value.Int(9),
			"key":       value.String("random"),
			"value":     value.Int(8),
		}),
		p: ProfileItem{
			OType:      "user",
			Oid:        "1",
			Key:        "random",
			Value:      value.Int(8),
			UpdateTime: 9000000,
		},
	}, {
		v: value.NewDict(map[string]value.Value{
			"oid":       value.String("aditya"),
			"otype":     value.String("user"),
			"timestamp": value.Int(9),
			"key":       value.String("random"),
			"value":     value.NewList(value.Int(8), value.String("abc")),
		}),
		p: ProfileItem{
			OType:      "user",
			Oid:        `"aditya"`,
			Key:        "random",
			Value:      value.NewList(value.Int(8), value.String("abc")),
			UpdateTime: 9000000,
		},
	}}
	for _, test := range tests {
		p, err := FromValueDict(test.v)
		assert.NoError(t, err)
		d, err := p.ToValueDict()
		assert.NoError(t, err)
		microTimestamp := test.v.GetUnsafe("timestamp").(value.Int)
		test.v.Set("timestamp", microTimestamp*1000000)
		assert.Equal(t, test.v, d)
		assert.Equal(t, test.p, p)
	}
}
