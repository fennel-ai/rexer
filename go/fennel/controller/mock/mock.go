package mock

import (
	"fennel/lib/profile"
	"fennel/lib/value"
)

type Data struct {
	Profiles []profile.ProfileItem `json:"profiles"`
}

var Store = make(map[int64]*Data)

func GetProfiles(reqs []profile.ProfileItem, id int64) []value.Value {
	var vals []value.Value
	mockedProfiles := Store[id].Profiles
	for _, p1 := range reqs {
		var latest uint64 = 0
		latestIdx := -1
		for i, p2 := range mockedProfiles {
			if p1.Oid == p2.Oid && p1.OType == p2.OType && p1.Key == p2.Key {
				if p1.Version == 0 {
					if p2.Version >= latest {
						latest = p2.Version
						latestIdx = i
					}
				} else if p1.Version == p2.Version {
					vals = append(vals, p2.Value)
					break
				}
			}
		}
		if p1.Version == 0 {
			if latestIdx == -1 {
				vals = append(vals, nil)
			} else {
				vals = append(vals, mockedProfiles[latestIdx].Value)
			}
		}
	}
	return vals
}
