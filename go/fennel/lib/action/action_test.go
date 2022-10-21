package action

import (
	"encoding/json"
	"fmt"
	"math"
	"testing"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/value"

	"github.com/stretchr/testify/assert"
)

func TestAction_ToFromValueDict(t *testing.T) {
	a := Action{
		ActionID:   1,
		ActorID:    "3",
		ActorType:  "user",
		TargetID:   `"f9rp2"`,
		TargetType: "video",
		ActionType: "like",
		Timestamp:  9,
		RequestID:  "10",
		Metadata:   value.Int(8),
	}
	expected := value.NewDict(map[string]value.Value{
		"action_id":   value.Int(1),
		"actor_id":    value.Int(3),
		"actor_type":  value.String("user"),
		"target_id":   value.String("f9rp2"),
		"target_type": value.String("video"),
		"action_type": value.String("like"),
		"timestamp":   value.Int(9),
		"request_id":  value.Int(10),
		"metadata":    value.Int(8),
	})
	d, err := a.ToValueDict()
	assert.NoError(t, err)
	a2, err := FromValueDict(d)
	assert.NoError(t, err)
	assert.True(t, a.Equals(a2, false))
	assert.NoError(t, err)
	assert.Equal(t, expected, d)
}

func TestToList(t *testing.T) {
	a1 := Action{ActionID: 1, ActorID: "3", ActorType: "user", TargetID: "5", TargetType: "photo", ActionType: "like", Timestamp: 9, RequestID: "10", Metadata: value.Int(8)}
	a2 := Action{ActionID: 11, ActorID: "13", ActorType: "other", TargetID: "15", TargetType: "video", ActionType: "myaction", Timestamp: 19, RequestID: "20", Metadata: value.Int(18)}
	a3 := Action{ActionID: 21, ActorID: "23", ActorType: "admin", TargetID: "25", TargetType: "arbitrary", ActionType: "share", Timestamp: 29, RequestID: "30", Metadata: value.Int(28)}
	expected := value.List{}
	d1, err := a1.ToValueDict()
	assert.NoError(t, err)
	d2, err := a2.ToValueDict()
	assert.NoError(t, err)
	d3, err := a3.ToValueDict()
	assert.NoError(t, err)
	expected.Append(d1, d2, d3)
	found, err := ToList([]Action{a1, a2, a3})
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func TestActionFromValueDict(t *testing.T) {
	tests := []struct {
		v value.Dict
		a Action
	}{{
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.Int(3),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(9),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}),
		a: Action{
			ActionID:   1,
			ActorID:    "3",
			ActorType:  "user",
			TargetID:   `"f9rp2"`,
			TargetType: "video",
			ActionType: "like",
			Timestamp:  9,
			RequestID:  "10",
			Metadata:   value.Int(8),
		},
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.Int(5),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(9),
			"request_id":  value.String("axd-asdff-sdfd"),
			"metadata":    value.Int(8),
		}),
		a: Action{
			ActionID:   1,
			ActorID:    `"aditya"`,
			ActorType:  "user",
			TargetID:   `5`,
			TargetType: "video",
			ActionType: "like",
			Timestamp:  9,
			RequestID:  `"axd-asdff-sdfd"`,
			Metadata:   value.Int(8),
		},
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(9),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}), a: Action{
			ActionID:   1,
			ActorID:    `"aditya"`,
			ActorType:  "user",
			TargetID:   `"f9rp2"`,
			TargetType: "video",
			ActionType: "like",
			Timestamp:  9,
			RequestID:  "10",
			Metadata:   value.Int(8),
		},
	}}
	for _, test := range tests {
		a, err := FromValueDict(test.v)
		assert.NoError(t, err)
		d, err := a.ToValueDict()
		assert.NoError(t, err)
		assert.Equal(t, test.v, d)
		assert.Equal(t, test.a, a)
	}
}

func TestActionTimestampResolved(t *testing.T) {
	ts := time.Now()
	tests := []struct {
		v value.Dict
		a Action
	}{{
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(ts.Unix()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}), a: Action{
			ActionID:   1,
			ActorID:    `"aditya"`,
			ActorType:  "user",
			TargetID:   `"f9rp2"`,
			TargetType: "video",
			ActionType: "like",
			Timestamp:  ftypes.Timestamp(ts.Unix()),
			RequestID:  "10",
			Metadata:   value.Int(8),
		},
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like2"),
			"timestamp":   value.Int(ts.UnixMilli()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}), a: Action{
			ActionID:   1,
			ActorID:    `"aditya"`,
			ActorType:  "user",
			TargetID:   `"f9rp2"`,
			TargetType: "video",
			ActionType: "like2",
			Timestamp:  ftypes.Timestamp(ts.Unix()),
			RequestID:  "10",
			Metadata:   value.Int(8),
		},
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like3"),
			"timestamp":   value.Int(ts.UnixMicro()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}), a: Action{
			ActionID:   1,
			ActorID:    `"aditya"`,
			ActorType:  "user",
			TargetID:   `"f9rp2"`,
			TargetType: "video",
			ActionType: "like3",
			Timestamp:  ftypes.Timestamp(ts.Unix()),
			RequestID:  "10",
			Metadata:   value.Int(8),
		},
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like4"),
			"timestamp":   value.Int(ts.UnixNano()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}), a: Action{
			ActionID:   1,
			ActorID:    `"aditya"`,
			ActorType:  "user",
			TargetID:   `"f9rp2"`,
			TargetType: "video",
			ActionType: "like4",
			Timestamp:  ftypes.Timestamp(ts.Unix()),
			RequestID:  "10",
			Metadata:   value.Int(8),
		}},
	}
	for _, test := range tests {
		a, err := FromValueDict(test.v)
		assert.NoError(t, err)
		assert.Equal(t, a, test.a)
	}
}

func TestActionFromFutureFails(t *testing.T) {
	ts := time.Now().Add(1 * time.Hour)
	tests := []struct {
		v value.Dict
	}{{
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(ts.Unix()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}),
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(ts.UnixMilli()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}),
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(ts.UnixMicro()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}),
	}, {
		v: value.NewDict(map[string]value.Value{
			"action_id":   value.Int(1),
			"actor_id":    value.String("aditya"),
			"actor_type":  value.String("user"),
			"target_id":   value.String("f9rp2"),
			"target_type": value.String("video"),
			"action_type": value.String("like"),
			"timestamp":   value.Int(ts.UnixNano()),
			"request_id":  value.Int(10),
			"metadata":    value.Int(8),
		}),
	}}

	for _, test := range tests {
		_, err := FromValueDict(test.v)
		assert.Error(t, err)
	}
}

func TestActionJSON(t *testing.T) {
	tests := []struct {
		str string
		a   Action
	}{{
		str: makeActionJSON(0, "", "", "", "", "", 0, "", "null"),
		a:   Action{Metadata: value.Nil},
	}, {
		str: makeActionJSON(1, "2", "3", "4", "5", "6", 7, "8", "9"),
		a:   Action{1, "2", "3", "4", "5", "6", 7, "8", value.Int(9)},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", "true"),
		a:   Action{Metadata: value.Bool(true)},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", "4.9"),
		a:   Action{Metadata: value.Double(4.9)},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", `"some string"`),
		a:   Action{Metadata: value.String("some string")},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", "[]"),
		a:   Action{Metadata: value.NewList()},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", "[1,{}]"),
		a:   Action{Metadata: value.NewList(value.Int(1), value.NewDict(nil))},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", `{}`),
		a:   Action{Metadata: value.NewDict(nil)},
	}, {
		str: makeActionJSON(0, "", "", "", "", "", 0, "", `{"key":"123"}`),
		a:   Action{Metadata: value.NewDict(map[string]value.Value{"key": value.String("123")})},
	}, {
		str: makeActionJSON(math.MaxUint64, "", "", "", "", "", math.MaxUint32, "", "null"),
		a:   Action{ActionID: math.MaxUint64, Timestamp: math.MaxUint32, Metadata: value.Nil},
	}}
	// Test unmarshal
	for _, tst := range tests {
		var a Action
		err := json.Unmarshal([]byte(tst.str), &a)
		assert.NoError(t, err)
		assert.True(t, tst.a.Equals(a, false))
	}
	// Test marshal
	for _, tst := range tests {
		ser, err := json.Marshal(tst.a)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(ser))
	}
}

func TestActionFetchRequestJSON(t *testing.T) {
	tests := []struct {
		str string
		afr ActionFetchRequest
	}{{
		str: makeActionFetchRequestJSON(0, 0, "", "", "", "", "", 0, 0, ""),
		afr: ActionFetchRequest{},
	}, {
		str: makeActionFetchRequestJSON(1, 2, "3", "4", "5", "6", "7", 8, 9, "10"),
		afr: ActionFetchRequest{1, 2, "3", "4", "5", "6", "7", 8, 9, "10"},
	}, {
		str: makeActionFetchRequestJSON(math.MaxUint64, math.MaxUint64, "", "", "", "", "",
			math.MaxUint32, math.MaxUint32, ""),
		afr: ActionFetchRequest{math.MaxUint64, math.MaxUint64, "", "", "", "", "",
			math.MaxUint32, math.MaxUint32, ""},
	}}
	// Test unmarshal
	for _, tst := range tests {
		var afr ActionFetchRequest
		err := json.Unmarshal([]byte(tst.str), &afr)
		assert.NoError(t, err)
		assert.Equal(t, tst.afr, afr)
	}
	// Test marshal
	for _, tst := range tests {
		ser, err := json.Marshal(tst.afr)
		assert.NoError(t, err)
		assert.Equal(t, tst.str, string(ser))
	}
}

func makeActionJSON(actionID uint64, actorID string, actorType string, targetID string, targetType string,
	actionType string, timestamp uint32, requestID string, metadata string) string {
	return fmt.Sprintf(`{"ActionID":%d,"ActorID":"%s","ActorType":"%s","TargetID":"%s","TargetType":"%s",`+
		`"ActionType":"%s","Timestamp":%d,"RequestID":"%s","Metadata":%s}`,
		actionID, actorID, actorType, targetID, targetType, actionType, timestamp, requestID, metadata)
}

func makeActionFetchRequestJSON(
	minActionID uint64, maxActionID uint64, actorID string, actorType string, targetID string, targetType string,
	actionType string, minTimestamp uint32, maxTimestamp uint32, requestID string) string {
	return fmt.Sprintf(
		`{"MinActionID":%d,"MaxActionID":%d,"ActorID":"%s","ActorType":"%s","TargetID":"%s","TargetType":"%s",`+
			`"ActionType":"%s","MinTimestamp":%d,"MaxTimestamp":%d,"RequestID":"%s"}`,
		minActionID, maxActionID, actorID, actorType, targetID, targetType,
		actionType, minTimestamp, maxTimestamp, requestID)
}
