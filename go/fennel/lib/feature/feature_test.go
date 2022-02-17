package feature

import (
	"encoding/json"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRow_Marshal(t *testing.T) {
	row := Row{"user", 1, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "modelname", 0.123}
	expected := `{"candidate_oid":31,"candidate_otype":"video","context_oid":1,"context_otype":"user","feature__f1":1,"feature__f2":null,"model_id":"modelname","model_prediction":0.123,"request_id":123,"timestamp":423,"workflow":"myworkflow"}`
	found, err := json.Marshal(row)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(found))
}

func TestRow_Marshal_Unmarshal_JSON(t *testing.T) {
	tests := []Row{
		{"user", 1, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.List{value.Int(2), value.Int(4)}}, "myworkflow", 123, 423, "modelname", 0.123},
		{"uid", 1e17, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "", 0},
		{"uid", 3, "video", ftypes.OidType(31), value.Dict{"f1_f3": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "modelname", 0.123},
		{"uid", 12, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "modelname", 0.123},
	}
	for _, test := range tests {
		b, err := json.Marshal(test)
		assert.NoError(t, err)
		var r Row
		err = r.UnmarshalJSON(b)
		assert.NoError(t, err)
		assert.Equal(t, test, r)
	}
}
func TestFrom_To_ProtoRow(t *testing.T) {
	tests := []Row{
		{"user", 1, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.List{value.Int(2), value.Int(4)}}, "myworkflow", 123, 423, "modelname", 0.123},
		{"uid", 1e17, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "", 0},
		{"uid", 3, "video", ftypes.OidType(31), value.Dict{"f1_f3": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "modelname", 0.123},
		{"uid", 12, "video", ftypes.OidType(31), value.Dict{"f1": value.Int(1), "f2": value.Nil}, "myworkflow", 123, 423, "modelname", 0.123},
	}
	for _, test := range tests {
		b, err := ToProto(test)
		assert.NoError(t, err)
		back, err := FromProtoRow(*b)
		assert.NoError(t, err)
		assert.Equal(t, test, *back)
	}
}
