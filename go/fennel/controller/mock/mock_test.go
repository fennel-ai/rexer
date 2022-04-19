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
	profiles = append(profiles, makeProfileItem("type1", "1", "key1", 1, value.Int(1)))
	profiles = append(profiles, makeProfileItem("type2", "2", "key2", 5, value.Int(3)))

	var reqs []profile.ProfileItemKey
	reqs = append(reqs, profiles[0].GetProfileKey())
	reqs = append(reqs, profiles[1].GetProfileKey())

	var id int64 = 79
	Store[id] = &Data{Profiles: profiles}

	exp := []value.Value{value.Int(1), value.Int(3)}
	found := GetProfiles(reqs, id)

	assert.Equal(t, len(exp), len(found))
	for i := range exp {
		assert.Equal(t, exp[i], found[i])
	}
}

func makeProfileItem(otype string, oid string, k string, updTime uint64, val value.Value) profile.ProfileItem {
	return profile.ProfileItem{
		OType:      ftypes.OType(otype),
		Oid:        oid,
		Key:        k,
		UpdateTime: updTime,
		Value:      val,
	}
}
