package action

import (
	"encoding/json"
	"fennel/lib/value"
	"fmt"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAction_ToValueDict(t *testing.T) {
	a := Action{
		ActionID:   1,
		ActorID:    3,
		ActorType:  "user",
		TargetID:   5,
		TargetType: "video",
		ActionType: "like",
		Timestamp:  9,
		RequestID:  10,
		Metadata:   value.Int(8),
	}
	expected := value.Dict{
		"action_id":   value.Int(1),
		"actor_id":    value.Int(3),
		"actor_type":  value.String("user"),
		"target_id":   value.Int(5),
		"target_type": value.String("video"),
		"action_type": value.String("like"),
		"timestamp":   value.Int(9),
		"request_id":  value.Int(10),
		"metadata":    value.Int(8),
	}
	assert.Equal(t, expected, a.ToValueDict())
}

func TestToTable(t *testing.T) {
	a1 := Action{ActionID: 1, ActorID: 3, ActorType: "user", TargetID: 5, TargetType: "photo", ActionType: "like", Timestamp: 9, RequestID: 10, Metadata: value.Int(8)}
	a2 := Action{ActionID: 11, ActorID: 13, ActorType: "other", TargetID: 15, TargetType: "video", ActionType: "myaction", Timestamp: 19, RequestID: 20, Metadata: value.Int(18)}
	a3 := Action{ActionID: 21, ActorID: 23, ActorType: "admin", TargetID: 25, TargetType: "arbitrary", ActionType: "share", Timestamp: 29, RequestID: 30, Metadata: value.Int(28)}
	expected := value.NewTable()
	assert.NoError(t, expected.Append(a1.ToValueDict()))
	assert.NoError(t, expected.Append(a2.ToValueDict()))
	assert.NoError(t, expected.Append(a3.ToValueDict()))
	found, err := ToTable([]Action{a1, a2, a3})
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func TestActionJSON(t *testing.T) {
	tests := []struct {
		str string
		a   Action
	}{{
		str: makeActionJSON(0, 0, "", 0, "", "", 0, 0, "null"),
		a:   Action{Metadata: value.Nil},
	}, {
		str: makeActionJSON(1, 2, "3", 4, "5", "6", 7, 8, "9"),
		a:   Action{1, 2, "3", 4, "5", "6", 7, 8, value.Int(9)},
	}, {
		str: makeActionJSON(0, 0, "", 0, "", "", 0, 0, "true"),
		a:   Action{Metadata: value.Bool(true)},
	}, {
		str: makeActionJSON(0, 0, "", 0, "", "", 0, 0, "4.9"),
		a:   Action{Metadata: value.Double(4.9)},
	}, {
		str: makeActionJSON(0, 0, "", 0, "", "", 0, 0, `"some string"`),
		a:   Action{Metadata: value.String("some string")},
	}, {
		str: makeActionJSON(0, 0, "", 0, "", "", 0, 0, "[1,[],{}]"),
		a:   Action{Metadata: value.List{value.Int(1), value.List{}, value.Dict{}}},
	}, {
		str: makeActionJSON(0, 0, "", 0, "", "", 0, 0, `{"key":"123"}`),
		a:   Action{Metadata: value.Dict{"key": value.String("123")}},
	}, {
		str: makeActionJSON(math.MaxUint64, math.MaxUint64, "", math.MaxUint64, "", "",
			math.MaxUint64, math.MaxUint64, "null"),
		a: Action{ActionID: math.MaxUint64, ActorID: math.MaxUint64, TargetID: math.MaxUint64,
			Timestamp: math.MaxUint64, RequestID: math.MaxUint64, Metadata: value.Nil},
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
		str: makeActionFetchRequestJSON(0, 0, 0, "", 0, "", "", 0, 0, 0),
		afr: ActionFetchRequest{},
	}, {
		str: makeActionFetchRequestJSON(1, 2, 3, "4", 5, "6", "7", 8, 9, 10),
		afr: ActionFetchRequest{1, 2, 3, "4", 5, "6", "7", 8, 9, 10},
	}, {
		str: makeActionFetchRequestJSON(math.MaxUint64, math.MaxUint64, math.MaxUint64, "", math.MaxUint64, "", "",
			math.MaxUint64, math.MaxUint64, math.MaxUint64),
		afr: ActionFetchRequest{math.MaxUint64, math.MaxUint64, math.MaxUint64, "", math.MaxUint64, "", "",
			math.MaxUint64, math.MaxUint64, math.MaxUint64},
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

func makeActionJSON(actionID uint64, actorID uint64, actorType string, targetID uint64, targetType string,
	actionType string, timestamp uint64, requestID uint64, metadata string) string {
	return fmt.Sprintf(`{"ActionID":%d,"ActorID":%d,"ActorType":"%s","TargetID":%d,"TargetType":"%s",`+
		`"ActionType":"%s","Timestamp":%d,"RequestID":%d,"Metadata":%s}`,
		actionID, actorID, actorType, targetID, targetType, actionType, timestamp, requestID, metadata)
}

func makeActionFetchRequestJSON(
	minActionID uint64, maxActionID uint64, actorID uint64, actorType string, targetID uint64, targetType string,
	actionType string, minTimestamp uint64, maxTimestamp uint64, requestID uint64) string {
	return fmt.Sprintf(
		`{"MinActionID":%d,"MaxActionID":%d,"ActorID":%d,"ActorType":"%s","TargetID":%d,"TargetType":"%s",`+
			`"ActionType":"%s","MinTimestamp":%d,"MaxTimestamp":%d,"RequestID":%d}`,
		minActionID, maxActionID, actorID, actorType, targetID, targetType,
		actionType, minTimestamp, maxTimestamp, requestID)
}
