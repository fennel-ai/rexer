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
		str: `{"OType":"","Oid":0,"Key":"","Value":null,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Nil},
	}, {
		str: `{"OType":"type1","Oid":2,"Key":"abc","Value":"xyz","UpdateTime":4}`,
		pi:  ProfileItem{OType: "type1", Oid: 2, Key: "abc", UpdateTime: 4, Value: value.String("xyz")},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Value":false,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Bool(false)},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Value":5,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Int(5)},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Value":3.14,"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.Double(3.14)},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Value":[],"UpdateTime":0}`,
		pi:  ProfileItem{Value: value.NewList()},
	},
		{
			str: `{"OType":"","Oid":0,"Key":"","Value":[[]],"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewList(value.NewList())},
		},
		{
			str: `{"OType":"","Oid":0,"Key":"","Value":[null],"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewList(value.Nil)},
		}, {
			str: `{"OType":"","Oid":0,"Key":"","Value":{},"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewDict(nil)},
		}, {
			str: `{"OType":"","Oid":0,"Key":"","Value":{"0":{}},"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewDict(map[string]value.Value{"0": value.NewDict(nil)})},
		}, {
			str: `{"OType":"","Oid":0,"Key":"","Value":{"k1":4.5},"UpdateTime":0}`,
			pi:  ProfileItem{Value: value.NewDict(map[string]value.Value{"k1": value.Double(4.5)})},
		}, {
			str: fmt.Sprintf(`{"OType":"","Oid":%d,"Key":"","Value":null,"UpdateTime":%d}`,
				uint64(math.MaxUint64), uint64(math.MaxUint64)),
			pi: ProfileItem{Oid: math.MaxUint64, UpdateTime: math.MaxUint64, Value: value.Nil},
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
