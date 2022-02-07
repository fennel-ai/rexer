package action

import (
	"fennel/lib/value"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAction_ToValueDict(t *testing.T) {
	a := Action{
		ActionID:   1,
		CustID:     2,
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
	a1 := Action{ActionID: 1, CustID: 2, ActorID: 3, ActorType: "user", TargetID: 5, TargetType: "photo", ActionType: "like", Timestamp: 9, RequestID: 10, Metadata: value.Int(8)}
	a2 := Action{ActionID: 11, CustID: 12, ActorID: 13, ActorType: "other", TargetID: 15, TargetType: "video", ActionType: "myaction", Timestamp: 19, RequestID: 20, Metadata: value.Int(18)}
	a3 := Action{ActionID: 21, CustID: 22, ActorID: 23, ActorType: "admin", TargetID: 25, TargetType: "arbitrary", ActionType: "share", Timestamp: 29, RequestID: 30, Metadata: value.Int(28)}
	expected := value.NewTable()
	assert.NoError(t, expected.Append(a1.ToValueDict()))
	assert.NoError(t, expected.Append(a2.ToValueDict()))
	assert.NoError(t, expected.Append(a3.ToValueDict()))
	found, err := ToTable([]Action{a1, a2, a3})
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}
