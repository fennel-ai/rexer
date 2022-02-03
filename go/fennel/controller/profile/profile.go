package profile

import (
	"fennel/instance"
	profilelib "fennel/lib/profile"
	"fennel/lib/value"
	"fennel/model/profile"
	"time"

	"google.golang.org/protobuf/proto"
)

func Get(this instance.Instance, request profilelib.ProfileItem) (value.Value, error) {
	if request.CustID == 0 {
		request.CustID = this.CustID
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	valueSer, err := profile.Get(this, request.CustID, request.OType, request.Oid, request.Key, request.Version)
	if err != nil {
		return nil, err
	} else if valueSer == nil {
		// i.e. no error but also value found
		return nil, nil
	}
	var pval value.PValue
	if err = proto.Unmarshal(valueSer, &pval); err != nil {
		return nil, err
	}
	val, err := value.FromProtoValue(&pval)
	if err != nil {
		return nil, err
	}
	return val, nil
}

func Set(this instance.Instance, request profilelib.ProfileItem) error {
	if request.CustID == 0 {
		request.CustID = this.CustID
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if request.Version == 0 {
		request.Version = uint64(time.Now().Unix())
	}
	pval, err := value.ToProtoValue(request.Value)
	if err != nil {
		return err
	}
	valSer, err := proto.Marshal(&pval)
	if err != nil {
		return err
	}
	if err = profile.Set(this, request.CustID, request.OType, request.Oid, request.Key, request.Version, valSer); err != nil {
		return err
	}
	return nil
}

func GetProfiles(this instance.Instance, request profilelib.ProfileFetchRequest) ([]profilelib.ProfileItem, error) {
	profilesSer, err := profile.GetProfiles(this, request)
	if err != nil {
		return nil, err
	}

	profiles := make([]profilelib.ProfileItem, 0)
	for _, prs := range profilesSer {
		pr, err := prs.ToProfileItem()
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, *pr)
	}

	return profiles, nil
}
