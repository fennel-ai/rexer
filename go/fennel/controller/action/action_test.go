package action

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/raulk/clock"

	"fennel/kafka"
	actionlib "fennel/lib/action"
	"fennel/lib/ftypes"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/model/action"
	"fennel/resource"
	"fennel/test"

	"github.com/stretchr/testify/assert"
)

func TestDBInsertFetch(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()
	ck := tier.Clock.(*clock.Mock)

	// initially fetching is empty
	actions, err := Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Empty(t, actions)

	t1 := ftypes.Timestamp(456)
	ck.Add(time.Duration(t1) * time.Second)

	a1 := getAction(1, "12", 0, "like")
	a2 := getAction(2, "22", t1+1, "like")
	assert.NoError(t, action.InsertBatch(ctx, tier, []actionlib.Action{a1, a2}))

	// and when time is not specified we use the current time to populate it
	a1.Timestamp = t1
	found, err := Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Len(t, found, 2)
	assert.True(t, a1.Equals(found[0], true))
	assert.True(t, a2.Equals(found[1], true))

	// insert couple more
	a3 := getAction(1, "12", t1+5, "like")
	a4 := getAction(2, "22", t1+7, "like")
	assert.NoError(t, action.InsertBatch(ctx, tier, []actionlib.Action{a3, a4}))

	found, err = Fetch(ctx, tier, actionlib.ActionFetchRequest{})
	assert.NoError(t, err)
	assert.Len(t, found, 4)
	assert.True(t, a1.Equals(found[0], true))
	assert.True(t, a2.Equals(found[1], true))
	assert.True(t, a3.Equals(found[2], true))
	assert.True(t, a4.Equals(found[3], true))
}

func testKafkaInsertRead(t *testing.T, batch bool) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	ck := tier.Clock.(*clock.Mock)
	t1 := ftypes.Timestamp(456)
	ck.Add(time.Duration(t1) * time.Second)

	a1 := getAction(1, "12", 0, "like")
	a2 := getAction(2, "22", t1, "like")
	if batch {
		assert.NoError(t, BatchInsert(ctx, tier, []actionlib.Action{a1, a2}))
	} else {
		assert.NoError(t, Insert(ctx, tier, a1))
		assert.NoError(t, Insert(ctx, tier, a2))
	}

	// when time is not specified we use the current time to populate it
	a1.Timestamp = t1
	actions := []actionlib.Action{a1, a2}

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer1, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
			Scope:        resource.NewTierScope(tier.ID),
			Topic:        actionlib.ACTIONLOG_KAFKA_TOPIC,
			GroupID:      utils.RandString(6),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
		assert.NoError(t, err)
		defer consumer1.Close()
		found, err := ReadBatch(ctx, consumer1, 2, time.Second*30)
		assert.NoError(t, err)
		assert.Equal(t, actions, found)
	}()

	// finally, transferring these from kafka to DB also works
	go func() {
		defer wg.Done()
		consumer2, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
			Scope:        resource.NewTierScope(tier.ID),
			Topic:        actionlib.ACTIONLOG_KAFKA_TOPIC,
			GroupID:      utils.RandString(6),
			OffsetPolicy: kafka.DefaultOffsetPolicy,
		})
		assert.NoError(t, err)
		defer consumer2.Close()
		assert.NoError(t, TransferToDB(ctx, tier, consumer2))
		found, err := Fetch(ctx, tier, actionlib.ActionFetchRequest{})
		assert.NoError(t, err)
		assert.Len(t, found, len(actions))
		for i, a := range actions {
			assert.True(t, a.Equals(found[i], true))
		}
	}()
	wg.Wait()

	// Test that actions were written as JSON as well.
	consumer, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tier.ID),
		Topic:        actionlib.ACTIONLOG_JSON_KAFKA_TOPIC,
		GroupID:      utils.RandString(6),
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	assert.NoError(t, err)
	defer consumer.Close()
	data, err := consumer.ReadBatch(ctx, 2, 30*time.Second)
	assert.NoError(t, err)
	found := make([]actionlib.Action, 0)
	for i := range data {
		var a actionlib.Action
		err := a.UnmarshalJSON(data[i])
		assert.NoError(t, err)
		found = append(found, a)
	}
	assert.Equal(t, actions, found)
}

func TestKafkaInsertRead(t *testing.T) {
	testKafkaInsertRead(t, false)
}

func TestKafkaBatchInsertRead(t *testing.T) {
	testKafkaInsertRead(t, true)
}

func TestLongTypes(t *testing.T) {
	// valid action
	a := actionlib.Action{
		ActorID:    "111",
		ActorType:  "11",
		TargetType: "12",
		TargetID:   "121",
		ActionType: "13",
		Metadata:   value.Int(14),
		Timestamp:  15,
		RequestID:  "16",
	}

	// ActionType can't be longer than 255 chars
	a.ActionType = ftypes.ActionType(utils.RandString(256))
	err := a.Validate()
	assert.Error(t, err)
	a.ActionType = ftypes.ActionType(utils.RandString(255))

	// ActorType can't be longer than 255 chars
	a.ActorType = ftypes.OType(utils.RandString(256))
	err = a.Validate()
	assert.Error(t, err)
	a.ActorType = ftypes.OType(utils.RandString(255))

	// TargetType can't be longer than 255 charks
	a.TargetType = ftypes.OType(utils.RandString(256))
	err = a.Validate()
	assert.Error(t, err)
	a.TargetType = ftypes.OType(utils.RandString(255))
}

func Test_ReadActions(t *testing.T) {
	tier := test.Tier(t)
	defer test.Teardown(tier)
	ctx := context.Background()

	actions := make([]actionlib.Action, 0)
	uid := ftypes.OidType("41")
	for i := 0; i < 100; i++ {
		a1 := getAction(i, uid, ftypes.Timestamp(i+1000), "like")
		a2 := getAction(i, uid, ftypes.Timestamp(i+1005), "share")
		assert.NoError(t, Insert(ctx, tier, a1))
		assert.NoError(t, Insert(ctx, tier, a2))
		actions = append(actions, a1, a2)
	}
	c1, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tier.ID),
		Topic:        actionlib.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      "one",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
	defer c1.Close()
	assert.NoError(t, err)
	c2, err := tier.NewKafkaConsumer(kafka.ConsumerConfig{
		Scope:        resource.NewTierScope(tier.ID),
		Topic:        actionlib.ACTIONLOG_KAFKA_TOPIC,
		GroupID:      "two",
		OffsetPolicy: kafka.DefaultOffsetPolicy,
	})
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
		ActionID:   ftypes.IDType(1 + i),
		ActorID:    uid,
		ActorType:  "user",
		TargetID:   "3",
		TargetType: "video",
		ActionType: actionType,
		Metadata:   value.Int(6),
		Timestamp:  ts,
		RequestID:  "7",
	}
}
