package profile

import (
	"context"
	profilelib "fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/model/profile"
	"fennel/tier"
	"time"

	"google.golang.org/protobuf/proto"
)

func Get(ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) (value.Value, error) {
	defer timer.Start(tier.ID, "controller.profile.get").ObserveDuration()
	if err := request.Validate(); err != nil {
		return nil, err
	}
	valueSer, err := profile.Get(ctx, tier, request.OType, request.Oid, request.Key, request.Version)
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

func Set(ctx context.Context, tier tier.Tier, request profilelib.ProfileItem) error {
	defer timer.Start(tier.ID, "controller.profile.set").ObserveDuration()
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
	if err = profile.Set(ctx, tier, request.OType, request.Oid, request.Key, request.Version, valSer); err != nil {
		return err
	}
	return nil
}

// GetBatched takes a list of profile items (value field is ignored) and returns a list of values
// corresponding to the value of each profile item. If profile item doesn't exist and hence the value
// is not found, nil is returned instead
func GetBatched(ctx context.Context, tier tier.Tier, requests []profilelib.ProfileItem) ([]value.Value, error) {
	sers, err := profile.GetBatched(ctx, tier, requests)
	if err != nil {
		return nil, err
	}
	ret := make([]value.Value, len(sers))
	for i := range sers {
		// if we don't have this data stored, well just return a nil
		if sers[i] == nil {
			ret[i] = nil
		} else {
			err = value.Unmarshal(sers[i], &ret[i])
			if err != nil {
				return nil, err
			}
		}
	}
	return ret, nil
}

func GetMulti(ctx context.Context, tier tier.Tier, request profilelib.ProfileFetchRequest) ([]profilelib.ProfileItem, error) {
	profilesSer, err := profile.GetMulti(ctx, tier, request)
	if err != nil {
		return nil, err
	}

	profiles, err := profilelib.FromProfileItemSerList(profilesSer)
	if err != nil {
		return nil, err
	}
	return profiles, nil
}
