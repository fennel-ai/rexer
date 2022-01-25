package profile

import (
	"fennel/instance"
	"fennel/lib/profile"
	"fennel/lib/value"
	"fennel/test"
	"testing"

	"github.com/stretchr/testify/assert"
)

func testProviderBasic(t *testing.T, p provider) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)
	val := value.Int(2)
	expected, _ := value.Marshal(val)

	// initially before setting, value isn't there so we get nil back
	// and calling get on a row that doesn't exist is not an error
	profile1 := profile.NewProfileItemSer(1, 1, 1232, "summary", 1, expected)
	verifyGet(t, p, this, profile1, []byte(nil))

	// now set the value
	verifySet(t, p, this, profile1, expected)

	// now get the same value back
	verifyGet(t, p, this, profile1, expected)

	// and get it again to verify nothing changes
	verifyGet(t, p, this, profile1, expected)

	// test getProfiles now
	request := profile.ProfileFetchRequest{}
	verifyMultiGet(t, this, request, []profile.ProfileItemSer{profile1})
}

func testProviderVersion(t *testing.T, p provider) {
	this, err := test.DefaultInstance()
	assert.NoError(t, err)

	profiles := make([]profile.ProfileItemSer, 0)
	request := profile.ProfileFetchRequest{}

	// initially table is empty
	verifyMultiGet(t, this, request, profiles)

	val1 := value.Int(2)
	expected1, _ := value.Marshal(val1)

	// first setting a version of 0 isn't possible
	err = p.set(this, 1, 1, 1232, "summary", 0, expected1)
	assert.Error(t, err)

	// but it works with a valid version
	profiles = append(profiles, profile.NewProfileItemSer(1, 1, 1232, "summary", 1, expected1))
	verifySet(t, p, this, profiles[0], expected1)
	verifyMultiGet(t, this, request, profiles)

	// and can set another version on the same value
	val2 := value.String("hello")
	expected2, _ := value.Marshal(val2)
	profiles = append(profiles, profile.NewProfileItemSer(1, 1, 1232, "summary", 2, expected2))
	verifySet(t, p, this, profiles[1], expected2)
	verifyMultiGet(t, this, request, profiles)

	// versions can also be non-continuous
	val3 := value.Dict(map[string]value.Value{
		"hi":  value.Int(1),
		"bye": value.List([]value.Value{value.Bool(true), value.String("yo")}),
	})
	expected3, _ := value.Marshal(val3)
	profiles = append(profiles, profile.NewProfileItemSer(1, 1, 1232, "summary", 10, expected3))
	verifySet(t, p, this, profiles[2], expected3)
	verifyMultiGet(t, this, request, profiles)

	// we can get any of these versions back
	verifyGet(t, p, this, profiles[0], expected1)
	verifyGet(t, p, this, profiles[1], expected2)
	verifyGet(t, p, this, profiles[2], expected3)

	// if we ask for version 0, by default get the highest version
	found, err := p.get(this, 1, 1, 1232, "summary", 0)
	assert.NoError(t, err)
	assert.Equal(t, expected3, found)

	// and asking for a version that doesn't exist return empty string
	found, err = p.get(this, 1, 1, 1232, "summary", 5)
	assert.NoError(t, err)
	assert.Equal(t, []byte(nil), found)
}

func verifySet(t *testing.T, p provider, this instance.Instance, pi profile.ProfileItemSer, valueSer []byte) {
	err := p.set(this, pi.CustID, pi.OType, pi.Oid, pi.Key, pi.Version, valueSer)
	assert.NoError(t, err)
}

func verifyGet(t *testing.T, p provider, this instance.Instance, pi profile.ProfileItemSer, expected []byte) {
	found, err := p.get(this, pi.CustID, pi.OType, pi.Oid, pi.Key, pi.Version)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}

func verifyMultiGet(t *testing.T, this instance.Instance, request profile.ProfileFetchRequest, expected []profile.ProfileItemSer) {
	found, err := GetProfiles(this, request)
	assert.NoError(t, err)
	assert.Equal(t, expected, found)
}
