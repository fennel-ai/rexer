package action

import (
	"context"
	"fmt"
	"testing"
	"time"

	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

func TestInsert(t *testing.T) {
	tier, err := test.Tier()
	defer test.Teardown(tier)
	assert.NoError(t, err)
	ctx := context.Background()

	// initially fetching is empty
	actions, err := Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Empty(t, actions)

	// insert actions and verify fetch works right away
	clock := test.FakeClock{}
	tier.Clock = &clock
	t1 := ftypes.Timestamp(456)
	clock.Set(int64(t1))
	a1 := actionlib.Action{ActorID: 1, ActorType: "myactor", TargetID: 2, TargetType: "mytarget", ActionType: "myaction", Metadata: value.Int(3), Timestamp: 0, RequestID: 4}
	aid1, err := Insert(ctx, tier, a1)
	assert.NoError(t, err)
	a1.ActionID = ftypes.OidType(aid1)
	// and when time is not specified we use the current time to populate it
	a1.Timestamp = t1
	actions, err = Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []actionlib.Action{a1}, actions)

	t2 := ftypes.Timestamp(1231)
	a2 := actionlib.Action{ActorID: 5, ActorType: "myactor", TargetID: 6, TargetType: "mytarget", ActionType: "myaction", Metadata: value.Int(7), Timestamp: t2, RequestID: 9}
	aid2, err := Insert(ctx, tier, a2)
	assert.NoError(t, err)
	a2.ActionID = ftypes.OidType(aid2)
	actions, err = Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Equal(t, []actionlib.Action{a1, a2}, actions)

	// and now verify that data has gone to kafka as well
	expected1, err := actionlib.ToProtoAction(a1)
	assert.NoError(t, err)
	var found actionlib.ProtoAction
	consumer, err := tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, "somegroup", "earliest")
	assert.NoError(t, err)
	defer consumer.Close()
	err = consumer.ReadProto(ctx, &found, time.Second*5)
	assert.NoError(t, err)
	assert.True(t, proto.Equal(&expected1, &found), fmt.Sprintf("Expected: %v, found: %v", expected1, found))

	expected2, err := actionlib.ToProtoAction(a2)
	assert.NoError(t, err)
	err = consumer.ReadProto(ctx, &found, time.Second*5)
	assert.NoError(t, err)
	assert.True(t, proto.Equal(&expected2, &found), fmt.Sprintf("Expected: %v, found: %v", expected2, found))
}
