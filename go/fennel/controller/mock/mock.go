package mock

import (
	"fennel/lib/profile"
	"fennel/lib/value"
)

type Data struct {
	Profiles []profile.ProfileItem `json:"profiles"`
}

var Store = make(map[int64]*Data)

func GetProfiles(reqs []profile.ProfileItemKey, id int64) []value.Value {
	var vals []value.Value
	mockedProfiles := Store[id].Profiles
	for _, pk := range reqs {
		for _, p := range mockedProfiles {
			if pk.Oid == p.Oid && pk.OType == p.OType && pk.Key == p.Key {
				vals = append(vals, p.Value)
			}
		}
	}
	return vals
}
