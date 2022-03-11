package profile

import (
	"context"
	"testing"
	"time"

	"fennel/kafka"
	profilelib "fennel/lib/profile"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

// TODO: Add more tests
func TestProfileController(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	vals := []value.Int{}
	for i := 0; i < 5; i++ {
		vals = append(vals, value.Int(i+1))
	}

	request := profilelib.ProfileFetchRequest{}
	profiles := []profilelib.ProfileItem{}
	profiles = append(profiles, profilelib.NewProfileItem("User", 1232, "summary", 1))
	profiles[0].Value = vals[0]

	// initially before setting, value isn't there so we get nil back
	// and calling get on a row that doesn't exist is not an error
	checkGet(t, ctx, tier, profiles[0], value.Nil)

	// no profiles exist initially
	checkGetMulti(t, ctx, tier, request, []profilelib.ProfileItem{})

	// cannot set an invalid profile
	err = Set(ctx, tier, profilelib.NewProfileItem("", 1, "key", 1))
	assert.Error(t, err)
	err = Set(ctx, tier, profilelib.NewProfileItem("User", 0, "key", 1))
	assert.Error(t, err)
	err = Set(ctx, tier, profilelib.NewProfileItem("User", 1, "", 1))
	assert.Error(t, err)

	// set a profile
	checkSet(t, ctx, tier, profiles[0])
	// test getting back the profile
	checkGet(t, ctx, tier, profiles[0], vals[0])
	// can get without using the specific version number
	profileTmp := profiles[0]
	profileTmp.Version = 0
	checkGet(t, ctx, tier, profileTmp, vals[0])
	checkGetMulti(t, ctx, tier, request, profiles)

	// set a few more profiles and verify it works
	profiles = append(profiles, profilelib.NewProfileItem("User", 1, "age", 2))
	profiles[1].Value = vals[1]
	checkSet(t, ctx, tier, profiles[1])
	checkGetMulti(t, ctx, tier, request, profiles)
	profiles = append(profiles, profilelib.NewProfileItem("User", 3, "age", 2))
	profiles[2].Value = vals[2]
	checkSet(t, ctx, tier, profiles[2])
	checkGetMulti(t, ctx, tier, request, profiles)
	checkGet(t, ctx, tier, profiles[1], vals[1])
	checkGet(t, ctx, tier, profiles[2], vals[2])
}

func TestProfileSetMultiWritesToKafka(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	profiles := []profilelib.ProfileItem{}
	profiles = append(profiles, profilelib.NewProfileItem("User", 1232, "summary", 1))
	profiles = append(profiles, profilelib.NewProfileItem("User", 1233, "summary foo", 10))
	profiles = append(profiles, profilelib.NewProfileItem("User", 1234, "summary", 12))
	profiles = append(profiles, profilelib.NewProfileItem("User", 1232, "summary2", 11))

	assert.NoError(t, SetMulti(ctx, tier, profiles))

	// Read kafka to check that profiles have been written
	found, err := readBatch(t, ctx, tier, 4)
	assert.NoError(t, err)
	assert.Equal(t, profiles, found)
}

func readBatch(t *testing.T, ctx context.Context, tier tier.Tier, count int) ([]profilelib.ProfileItem, error) {
	consumer, err := tier.NewKafkaConsumer(profilelib.PROFILELOG_KAFKA_TOPIC, utils.RandString(6), kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	defer consumer.Close()

	msgs, err := consumer.ReadBatch(ctx, count, time.Second*30)
	assert.NoError(t, err)
	actual := make([]profilelib.ProfileItem, len(msgs))
	for i := range msgs {
		var p profilelib.ProtoProfileItem
		if err = proto.Unmarshal(msgs[i], &p); err != nil {
			return nil, err
		}
		if actual[i], err = profilelib.FromProtoProfileItem(&p); err != nil {
			return nil, err
		}
	}
	return actual, nil
}

func checkSet(t *testing.T, ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) {
	err := Set(ctx, tier, request)
	assert.NoError(t, err)
}

func checkGet(t *testing.T, ctx context.Context, tier tier.Tier, request profilelib.ProfileItem, expected value.Value) {
	found, err := Get(ctx, tier, request)
	assert.NoError(t, err)
	// any test necessary for found == nil?
	if found != nil {
		assert.Equal(t, expected, found)
	}
}

func checkGetMulti(t *testing.T, ctx context.Context, tier tier.Tier, request profilelib.ProfileFetchRequest, expected []profilelib.ProfileItem) {
	found, err := GetMulti(ctx, tier, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, found)
}

func TestGetBatched(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: uint64(1), Key: "summary", Version: 1, Value: vals[0]},
		{OType: "User", Oid: uint64(2), Key: "summary", Version: 1, Value: vals[1]},
		{OType: "User", Oid: uint64(3), Key: "summary", Version: 1, Value: vals[2]},
	}

	// initially nothing exists
	found, err := GetBatched(ctx, tier, profiles)
	assert.NoError(t, err)
	assert.Equal(t, []value.Value{nil, nil, nil}, found)

	// set a few
	checkSet(t, ctx, tier, profiles[0])
	checkSet(t, ctx, tier, profiles[1])
	checkSet(t, ctx, tier, profiles[2])

	found, err = GetBatched(ctx, tier, profiles)
	assert.NoError(t, err)
	assert.Equal(t, vals, found)
}
