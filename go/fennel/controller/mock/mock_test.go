package mock

import (
	"testing"

	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/value"
	"github.com/stretchr/testify/assert"
)

func TestGetProfiles(t *testing.T) {
	var profiles []profile.ProfileItem
	profiles = append(profiles, makeProfileItem("type1", 1, "key1", 1, value.Int(1)))
	profiles = append(profiles, makeProfileItem("type1", 1, "key1", 7, value.Int(2)))
	profiles = append(profiles, makeProfileItem("type2", 2, "key2", 5, value.Int(3)))

	var reqs []profile.ProfileItem
	reqs = append(reqs, profiles[0])
	reqs = append(reqs, editProfile(profiles[0], 0))
	reqs = append(reqs, profiles[2])

	var id int64 = 79
	Store[id] = &Data{Profiles: profiles}

	exp := []value.Value{value.Int(1), value.Int(2), value.Int(3)}
	found := GetProfiles(reqs, id)

	assert.Equal(t, len(exp), len(found))
	for i := range exp {
		assert.True(t, exp[i].Equal(found[i]))
	}
}

func makeProfileItem(otype string, oid uint64, k string, ver uint64, val value.Value) profile.ProfileItem {
	return profile.ProfileItem{
		OType:   ftypes.OType(otype),
		Oid:     oid,
		Key:     k,
		Version: ver,
		Value:   val,
	}
}

func editProfile(p profile.ProfileItem, version uint64) profile.ProfileItem {
	return profile.ProfileItem{
		OType:   p.OType,
		Oid:     p.Oid,
		Key:     p.Key,
		Version: version,
		Value:   nil,
	}
}
