package profile

import (
	"encoding/json"
	"fennel/lib/value"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestProfileItemJSON(t *testing.T) {
	tests := []struct {
		str string
		pi  ProfileItem
	}{{
		str: `{"OType":"","Oid":0,"Key":"","Version":0,"Value":null}`,
		pi:  ProfileItem{Value: value.Nil},
	}, {
		str: `{"OType":"type1","Oid":1,"Key":"abc","Version":7,"Value":"xyz"}`,
		pi:  ProfileItem{OType: "type1", Oid: 1, Key: "abc", Version: 7, Value: value.String("xyz")},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Version":0,"Value":false}`,
		pi:  ProfileItem{Value: value.Bool(false)},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Version":0,"Value":5}`,
		pi:  ProfileItem{Value: value.Int(5)},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Version":0,"Value":3.14}`,
		pi:  ProfileItem{Value: value.Double(3.14)},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Version":0,"Value":[null]}`,
		pi:  ProfileItem{Value: value.List{value.Nil}},
	}, {
		str: `{"OType":"","Oid":0,"Key":"","Version":0,"Value":{"k1":4.5}}`,
		pi:  ProfileItem{Value: value.Dict{"k1": value.Double(4.5)}},
	}, {
		str: fmt.Sprintf(`{"OType":"","Oid":%d,"Key":"","Version":%d,"Value":null}`,
			uint64(math.MaxUint64), uint64(math.MaxUint64)),
		pi: ProfileItem{Oid: math.MaxUint64, Version: math.MaxUint64, Value: value.Nil},
	}}
	// Test unmarshal
	for _, tst := range tests {
		var pi ProfileItem
		err := json.Unmarshal([]byte(tst.str), &pi)
		assert.NoError(t, err)
		assert.True(t, tst.pi.Equals(&pi))
	}
	// Test marshal
	for _, tst := range tests {
		ser, err := json.Marshal(tst.pi)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(ser))
	}
}

func TestProfileFetchRequestJSON(t *testing.T) {
	tests := []struct {
		str string
		pfr ProfileFetchRequest
	}{{
		str: `{"OType":"","Oid":0,"Key":"","Version":0}`,
		pfr: ProfileFetchRequest{},
	}, {
		str: `{"OType":"type1","Oid":1,"Key":"abc","Version":7}`,
		pfr: ProfileFetchRequest{OType: "type1", Oid: 1, Key: "abc", Version: 7},
	}, {
		str: fmt.Sprintf(`{"OType":"","Oid":%d,"Key":"","Version":%d}`, uint64(math.MaxUint64), uint64(math.MaxUint64)),
		pfr: ProfileFetchRequest{Oid: uint64(math.MaxUint64), Version: uint64(math.MaxUint64)},
	}}
	// Test unmarshal
	for _, tst := range tests {
		var pfr2 ProfileFetchRequest
		err := json.Unmarshal([]byte(tst.str), &pfr2)
		assert.NoError(t, err)
		assert.Equal(t, tst.pfr, pfr2)
	}
	// Test marshal
	for _, tst := range tests {
		ser, err := json.Marshal(tst.pfr)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(ser))
	}
}
