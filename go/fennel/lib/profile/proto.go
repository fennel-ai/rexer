package profile

import (
	"fennel/lib/value"
)

func FromProtoProfileItem(ppr *ProtoProfileItem) (ProfileItem, error) {
	v, err := value.FromProtoValue(ppr.Value)
	if err != nil {
		return ProfileItem{}, err
	}
	return ProfileItem{
		ppr.OType,
		ppr.Oid,
		ppr.Key,
		v,
		ppr.Version,
	}, nil
}

func ToProtoProfileItem(pi *ProfileItem) (ProtoProfileItem, error) {
	pv, err := value.ToProtoValue(pi.Value)
	if err != nil {
		return ProtoProfileItem{}, err
	}
	return ProtoProfileItem{
		OType:   pi.OType,
		Oid:     pi.Oid,
		Key:     pi.Key,
		Version: pi.UpdateTime,
		Value:   &pv,
	}, nil
}

func FromProtoProfileList(profileList *ProtoProfileList) ([]ProfileItem, error) {
	profiles := make([]ProfileItem, len(profileList.Profiles))
	for i, ppr := range profileList.Profiles {
		var err error
		profiles[i], err = FromProtoProfileItem(ppr)
		if err != nil {
			return nil, err
		}
	}
	return profiles, nil
}

func ToProtoProfileList(profiles []ProfileItem) (*ProtoProfileList, error) {
	ret := &ProtoProfileList{}
	ret.Profiles = make([]*ProtoProfileItem, len(profiles))
	for i, profile := range profiles {
		ppr, err := ToProtoProfileItem(&profile)
		if err != nil {
			return nil, err
		}
		ret.Profiles[i] = &ppr
	}
	return ret, nil
}
