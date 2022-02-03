package profile

import (
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/plane"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: Add more tests
func TestProfileController(t *testing.T) {
	this, err := test.MockPlane()
	assert.NoError(t, err)

	vals := []value.Int{}
	for i := 0; i < 5; i++ {
		vals = append(vals, value.Int(i+1))
	}

	request := profilelib.ProfileFetchRequest{}
	profiles := []profilelib.ProfileItem{}
	profiles = append(profiles, profilelib.NewProfileItem(1, "User", 1232, "summary", 1))
	profiles[0].Value = vals[0]

	// initially before setting, value isn't there so we get nil back
	// and calling get on a row that doesn't exist is not an error
	checkGet(t, this, profiles[0], value.Nil)

	// no profiles exist initially
	checkMultiGet(t, this, request, []profilelib.ProfileItem{})

	// cannot set an invalid profile
	err = Set(this, profilelib.NewProfileItem(1, "", 1, "key", 1))
	assert.Error(t, err)
	err = Set(this, profilelib.NewProfileItem(1, "User", 0, "key", 1))
	assert.Error(t, err)
	err = Set(this, profilelib.NewProfileItem(1, "User", 1, "", 1))
	assert.Error(t, err)

	// set a profile
	checkSet(t, this, profiles[0])
	// test getting back the profile
	checkGet(t, this, profiles[0], vals[0])
	// can get without using the specific version number
	profileTmp := profiles[0]
	profileTmp.Version = 0
	checkGet(t, this, profileTmp, vals[0])
	checkMultiGet(t, this, request, profiles)

	// set a few more profiles and verify it works
	profiles = append(profiles, profilelib.NewProfileItem(1, "User", 1, "age", 2))
	profiles[1].Value = vals[1]
	checkSet(t, this, profiles[1])
	checkMultiGet(t, this, request, profiles)
	profiles = append(profiles, profilelib.NewProfileItem(1, "User", 3, "age", 2))
	profiles[2].Value = vals[2]
	checkSet(t, this, profiles[2])
	checkMultiGet(t, this, request, profiles)
	checkGet(t, this, profiles[1], vals[1])
	checkGet(t, this, profiles[2], vals[2])
}

func checkSet(t *testing.T, this plane.Plane, request profilelib.ProfileItem) {
	err := Set(this, request)
	assert.NoError(t, err)
}

func checkGet(t *testing.T, this plane.Plane, request profilelib.ProfileItem, expected value.Value) {
	found, err := Get(this, request)
	assert.NoError(t, err)
	// any test necessary for found == nil?
	if found != nil {
		assert.Equal(t, expected, found)
	}
}

func checkMultiGet(t *testing.T, this plane.Plane, request profilelib.ProfileFetchRequest, expected []profilelib.ProfileItem) {
	found, err := GetProfiles(this, request)
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, found)
}
