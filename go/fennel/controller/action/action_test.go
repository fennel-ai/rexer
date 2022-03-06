package action

import (
	"context"
	"testing"
	"time"

	"fennel/kafka"
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestDBInsertFetch(t *testing.T) {
	tier, err := test.Tier()
	defer test.Teardown(tier)
	assert.NoError(t, err)
	ctx := context.Background()

	// initially fetching is empty
	actions, err := Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Empty(t, actions)

	clock := test.FakeClock{}
	tier.Clock = &clock
	t1 := ftypes.Timestamp(456)
	clock.Set(int64(t1))

	a1 := getAction(1, 12, 0, "like")
	a2 := getAction(2, 22, t1+1, "like")
	assert.NoError(t, dbInsert(ctx, tier, []actionlib.Action{a1, a2}))

	// and when time is not specified we use the current time to populate it
	a1.Timestamp = t1
	found, err := Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Len(t, found, 2)
	assert.True(t, a1.Equals(found[0], true))
	assert.True(t, a2.Equals(found[1], true))

	// insert couple more
	a3 := getAction(1, 12, t1+5, "like")
	a4 := getAction(2, 22, t1+7, "like")
	assert.NoError(t, dbInsert(ctx, tier, []actionlib.Action{a3, a4}))

	found, err = Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Len(t, found, 4)
	assert.True(t, a1.Equals(found[0], true))
	assert.True(t, a2.Equals(found[1], true))
	assert.True(t, a3.Equals(found[2], true))
	assert.True(t, a4.Equals(found[3], true))
}

func testKafkaInsertRead(t *testing.T, batch bool) {
	tier, err := test.Tier()
	defer test.Teardown(tier)
	assert.NoError(t, err)
	ctx := context.Background()

	// insert actions and verify fetch works right away
	clock := test.FakeClock{}
	tier.Clock = &clock
	t1 := ftypes.Timestamp(456)
	clock.Set(int64(t1))

	// and now verify that data has gone to kafka as well
	a1 := getAction(1, 12, 0, "like")
	a2 := getAction(2, 22, t1, "like")
	if batch {
		assert.NoError(t, BatchInsert(ctx, tier, []actionlib.Action{a1, a2}))
	} else {
		assert.NoError(t, Insert(ctx, tier, a1))
		assert.NoError(t, Insert(ctx, tier, a2))
	}

	// when time is not specified we use the current time to populate it
	a1.Timestamp = t1
	actions := []actionlib.Action{a1, a2}

	consumer1, err := tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, "somegroup", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer consumer1.Close()
	found, err := ReadBatch(ctx, consumer1, 2, time.Second*30)
	assert.NoError(t, err)
	assert.Equal(t, actions, found)

	// finally, transferring these from kafka to DB also works
	consumer2, err := tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, "insert_in_db", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer consumer2.Close()
	assert.NoError(t, TransferToDB(ctx, tier, consumer2))
	found, err = Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	for i, a := range actions {
		assert.True(t, a.Equals(found[i], true))
	}
}

func TestKafkaInsertRead(t *testing.T) {
	testKafkaInsertRead(t /* batch= */, false)
}

func TestKafkaBatchInsertRead(t *testing.T) {
	testKafkaInsertRead(t /* batch= */, true)
}

func TestLongTypes(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)

	ctx := context.Background()
	// valid action
	a := actionlib.Action{
		ActorID:    111,
		ActorType:  "11",
		TargetType: "12",
		TargetID:   121,
		ActionType: "13",
		Metadata:   value.Int(14),
		Timestamp:  15,
		RequestID:  16,
	}

	// ActionType can't be longer than 255 chars
	a.ActionType = ftypes.ActionType(utils.RandString(256))
	err = Insert(ctx, tier, a)
	assert.Error(t, err)
	a.ActionType = ftypes.ActionType(utils.RandString(255))

	// ActorType can't be longer than 255 chars
	a.ActorType = ftypes.OType(utils.RandString(256))
	err = Insert(ctx, tier, a)
	assert.Error(t, err)
	a.ActorType = ftypes.OType(utils.RandString(255))

	// TargetType can't be longer than 255 charks
	a.TargetType = ftypes.OType(utils.RandString(256))
	err = Insert(ctx, tier, a)
	assert.Error(t, err)
	a.TargetType = ftypes.OType(utils.RandString(255))
}

func Test_ReadActions(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	actions := make([]actionlib.Action, 0)
	uid := ftypes.OidType(41)
	for i := 0; i < 100; i++ {
		a1 := getAction(i, uid, ftypes.Timestamp(i+1000), "like")
		a2 := getAction(i, uid, ftypes.Timestamp(i+1005), "share")
		assert.NoError(t, Insert(ctx, tier, a1))
		assert.NoError(t, Insert(ctx, tier, a2))
		actions = append(actions, a1, a2)
	}
	c1, err := tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, "one", kafka.DefaultOffsetPolicy)
	defer c1.Close()
	assert.NoError(t, err)
	c2, err := tier.NewKafkaConsumer(actionlib.ACTIONLOG_KAFKA_TOPIC, "two", kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer c2.Close()

	// verify both c1 and c2 produce the same actions
	found1, err := ReadBatch(ctx, c1, 200, time.Second*30)
	assert.NoError(t, err)
	assert.Equal(t, found1, actions)

	found2, err := ReadBatch(ctx, c2, 200, time.Second*30)
	assert.NoError(t, err)
	assert.Equal(t, found2, actions)
}

func getAction(i int, uid ftypes.OidType, ts ftypes.Timestamp, actionType ftypes.ActionType) actionlib.Action {
	return actionlib.Action{
		ActionID:   ftypes.OidType(1 + i),
		ActorID:    uid,
		ActorType:  "user",
		TargetID:   3,
		TargetType: "video",
		ActionType: actionType,
		Metadata:   value.Int(6),
		Timestamp:  ts,
		RequestID:  7,
	}
}
