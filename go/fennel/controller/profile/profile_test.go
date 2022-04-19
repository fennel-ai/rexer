package profile

import (
	"context"
	"testing"
	"time"

	"fennel/kafka"
	profilelib "fennel/lib/profile"
	"fennel/lib/utils"
	"fennel/lib/value"
	"fennel/model/profile"
	"fennel/test"
	"fennel/tier"

	"github.com/stretchr/testify/assert"
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

	//request := profilelib.ProfileItemKey{}
	profiles := []profilelib.ProfileItem{}
	profiles = append(profiles, profilelib.NewProfileItem("User", 1232, "summary", value.Int(1), 1))
	profiles[0].Value = vals[0]

	// initially before setting, value isn't there so we get nil back
	// and calling get on a row that doesn't exist is not an error
	checkGet(t, ctx, tier, profiles[0].GetProfileKey(), value.Nil)

	// cannot set an invalid profile
	err = Set(ctx, tier, profilelib.NewProfileItem("", 1, "key", value.Int(1), 1))
	assert.Error(t, err)
	err = Set(ctx, tier, profilelib.NewProfileItem("User", 0, "key", value.Int(1), 1))
	assert.Error(t, err)
	err = Set(ctx, tier, profilelib.NewProfileItem("User", 1, "", value.Int(1), 1))
	assert.Error(t, err)

	// set a profile
	checkSet(t, ctx, tier, profiles[0])
	// test getting back the profile
	checkGet(t, ctx, tier, profiles[0].GetProfileKey(), vals[0])
	// test that the profile was written to kafka queue as well
	consumer, err := tier.NewKafkaConsumer(profilelib.PROFILELOG_KAFKA_TOPIC, utils.RandString(6), kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	found, err := readBatch(ctx, consumer, 1, time.Second*2)
	assert.NoError(t, err)
	assert.Equal(t, profiles, found)

	// can get without using the specific version number
	profileTmp := profiles[0]
	profileTmp.UpdateTime = 0
	checkGet(t, ctx, tier, profileTmp.GetProfileKey(), vals[0])
	// set a few more profiles and verify it works
	profiles = append(profiles, profilelib.NewProfileItem("User", 1, "age", value.Int(2), 0))
	profiles[1].Value = vals[1]
	checkSet(t, ctx, tier, profiles[1])
	profiles = append(profiles, profilelib.NewProfileItem("User", 3, "age", value.Int(2), 0))
	profiles[2].Value = vals[2]
	checkSet(t, ctx, tier, profiles[2])

	checkGet(t, ctx, tier, profiles[1].GetProfileKey(), vals[1])
	checkGet(t, ctx, tier, profiles[2].GetProfileKey(), vals[2])
}

func TestProfileDBInsert(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test behavior across
	// different objects in `_integration_test`

	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3), value.Int(4)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: 1222, Key: "summary", UpdateTime: 1, Value: vals[0]},
		{OType: "User", Oid: 1222, Key: "summary", UpdateTime: 10, Value: vals[1]},
		{OType: "User", Oid: 1222, Key: "summary", UpdateTime: 12, Value: vals[2]},
		{OType: "User", Oid: 1222, Key: "summary", UpdateTime: 11, Value: vals[3]},
	}

	assert.NoError(t, profile.SetBatch(ctx, tier, profiles))

	pks := []profilelib.ProfileItemKey{}
	for _, p := range profiles {
		pks = append(pks, p.GetProfileKey())
	}
	exp := profilelib.NewProfileItem("User", 1222, "summary", vals[2], 0)
	// check that the entries were written
	actual, err := GetBatch(ctx, tier, pks)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []profilelib.ProfileItem{exp, exp, exp, exp}, actual)

	v, err := GetBatch(ctx, tier, []profilelib.ProfileItemKey{
		{OType: "User", Oid: 1222, Key: "summary"},
		{OType: "User", Oid: 1222, Key: "summary"},
	})
	assert.NoError(t, err)
	assert.ElementsMatch(t, []profilelib.ProfileItem{exp, exp}, v)
}

func TestProfileSetMultiWritesToKafka(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	profiles := []profilelib.ProfileItem{}
	profiles = append(profiles, profilelib.NewProfileItem("User", 1232, "summary", value.Int(1), 2))
	profiles = append(profiles, profilelib.NewProfileItem("User", 1233, "summary foo", value.Int(10), 2))
	profiles = append(profiles, profilelib.NewProfileItem("User", 1234, "summary", value.Int(12), 2))
	profiles = append(profiles, profilelib.NewProfileItem("User", 1232, "summary2", value.Int(11), 2))

	assert.NoError(t, SetMulti(ctx, tier, profiles))

	// Read kafka to check that profiles have been written
	consumer, err := tier.NewKafkaConsumer(profilelib.PROFILELOG_KAFKA_TOPIC, utils.RandString(6), kafka.DefaultOffsetPolicy)
	assert.NoError(t, err)
	found, err := readBatch(ctx, consumer, 4, time.Second*10)
	assert.NoError(t, err)
	assert.Equal(t, profiles, found)
}

func checkSet(t *testing.T, ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) {
	err := Set(ctx, tier, request)
	assert.NoError(t, err)
}

func checkGet(t *testing.T, ctx context.Context, tier tier.Tier, request profilelib.ProfileItemKey, expected value.Value) {
	found, err := Get(ctx, tier, request)
	assert.NoError(t, err)
	// any test necessary for found == nil?
	if found.Value != value.Nil {
		assert.Equal(t, expected, found.Value)
	}
}

func TestGetBatched(t *testing.T) {
	tier, err := test.Tier()
	assert.NoError(t, err)
	defer test.Teardown(tier)
	ctx := context.Background()

	// mini-redis does not play well with cache keys in different "slots" (in the same txn),
	// currently it is determined using (otype, oid, key). We test behavior across
	// different objects in `_integration_test`
	vals := []value.Value{value.Int(1), value.Int(2), value.Int(3)}
	profiles := []profilelib.ProfileItem{
		{OType: "User", Oid: uint64(1), Key: "summary", UpdateTime: 1, Value: vals[0]},
		{OType: "User", Oid: uint64(1), Key: "summary", UpdateTime: 2, Value: vals[1]},
		{OType: "User", Oid: uint64(1), Key: "summary", UpdateTime: 3, Value: vals[2]},
	}

	pks := make([]profilelib.ProfileItemKey, len(profiles))
	for i, p := range profiles {
		pks[i] = profilelib.ProfileItemKey{OType: p.OType, Oid: p.Oid, Key: p.Key}
	}

	nilProfile := profilelib.ProfileItem{OType: "User", Oid: uint64(1), Key: "summary", UpdateTime: 0, Value: value.Nil}
	// initially nothing exists
	found, err := GetBatch(ctx, tier, pks)
	assert.NoError(t, err)
	assert.Equal(t, []profilelib.ProfileItem{nilProfile, nilProfile, nilProfile}, found)

	// set a few
	checkSet(t, ctx, tier, profiles[0])
	checkSet(t, ctx, tier, profiles[1])
	checkSet(t, ctx, tier, profiles[2])

	found, err = GetBatch(ctx, tier, pks)
	assert.NoError(t, err)
	expectedProfile := profilelib.ProfileItem{OType: "User", Oid: uint64(1), Key: "summary", UpdateTime: 0, Value: vals[2]}
	assert.Equal(t, []profilelib.ProfileItem{expectedProfile, expectedProfile, expectedProfile}, found)
}
