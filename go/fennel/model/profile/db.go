package profile

import (
	"context"
	"fmt"
	"strings"
	"time"

	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/lib/value"
	"fennel/tier"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

// we create a private interface to make testing caching easier
type provider interface {
	set(ctx context.Context, tier tier.Tier, profile profile.ProfileItem) error
	setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItem) error
	get(ctx context.Context, tier tier.Tier, profileKey profile.ProfileItemKey) (profile.ProfileItem, error)
	getBatch(ctx context.Context, tier tier.Tier, profileKeys []profile.ProfileItemKey) ([]profile.ProfileItem, error)
}

type dbProvider struct{}

var _ provider = dbProvider{}

// This struct is internal to db.go and is only used for reading values b/w server and DB.
// DONOT use this for anything else.
type profileItemSer struct {
	OType      ftypes.OType `db:"otype"`
	Oid        string       `db:"oid"`
	Key        string       `db:"zkey"`
	UpdateTime uint64       `db:"version"`
	Value      []byte       `db:"value"`
}

func (ser *profileItemSer) toProfileItem() (profile.ProfileItem, error) {
	pr := profile.NewProfileItem(string(ser.OType), ser.Oid, ser.Key, value.Nil, ser.UpdateTime)
	val, err := value.FromJSON(ser.Value)
	if err != nil {
		return pr, err
	}
	pr.Value = val
	return pr, nil
}

func toProfileItemSer(profile profile.ProfileItem) *profileItemSer {
	return &profileItemSer{
		OType:      profile.OType,
		Oid:        profile.Oid,
		Key:        profile.Key,
		UpdateTime: profile.UpdateTime,
		Value:      value.ToJSON(profile.Value),
	}
}

func (D dbProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "model.profile.db.setBatch")
	defer t.Stop()
	if len(profiles) == 0 {
		return nil
	}

	latestProfileByKey := make(map[profile.ProfileItemKey]profile.ProfileItem)
	for _, p := range profiles {
		pk := p.GetProfileKey()
		if err := p.Validate(); err != nil {
			return fmt.Errorf("invalid profile: %v", err)
		}
		if p.UpdateTime == 0 {
			p.UpdateTime = uint64(time.Now().UnixMicro())
		}
		val, ok := latestProfileByKey[pk]
		if !ok {
			latestProfileByKey[pk] = p
		} else {
			if p.UpdateTime > val.UpdateTime {
				latestProfileByKey[pk] = p
			}
		}
	}

	serializedProfiles := make([]profileItemSer, 0, len(profiles))
	for _, prof := range latestProfileByKey {
		pSer := toProfileItemSer(prof)
		serializedProfiles = append(serializedProfiles, *pSer)
	}

	// validate profiles
	for _, profile := range serializedProfiles {
		if profile.UpdateTime == 0 {
			return fmt.Errorf("version can not be zero")
		}
		if len(profile.Key) > 255 {
			return fmt.Errorf("Key too long: keys can only be upto 255 chars")
		}
		if len(profile.OType) > 255 {
			return fmt.Errorf("otype too long: otypes can only be upto 255 chars")
		}
	}

	// write
	sql := `
		INSERT INTO profile
			(otype, oid, zkey, version, value)
		VALUES `
	vals := make([]interface{}, 0)
	for _, profile := range serializedProfiles {
		sql += "(?, ?, ?, ?, ?),"
		vals = append(vals, profile.OType, profile.Oid, profile.Key, profile.UpdateTime, profile.Value)
	}
	sql = strings.TrimSuffix(sql, ",") // remove the last trailing comma

	// this is to simulate `insert only if the timestamp for the update is newer than the existing one`
	// semantics in SQL.
	//
	// NOTE: This may result in unexpected behavior is the user tries to set a different
	// value with the same update time stamp. Previously we had adopted failing such requests, but to
	// make the behavior idempotent, we do not fail the request but retain the value as is.
	// We expect the user to not set the update timestamp in case of such updates.
	//
	// NOTE: any AUTO_INCREMENT columns are incremented if UPDATE path is triggered

	update_clause := `
	 ON DUPLICATE KEY UPDATE 
	 value = IF(version < VALUES(version), VALUES(value), value),
	 version = IF(version < VALUES(version), VALUES(version), version)
	`
	sql += update_clause
	_, err := tier.DB.ExecContext(ctx, sql, vals...)
	return err
}

func (D dbProvider) set(ctx context.Context, tier tier.Tier, profileItem profile.ProfileItem) error {
	ctx, t := timer.Start(ctx, tier.ID, "model.profile.db.set")
	defer t.Stop()
	return D.setBatch(ctx, tier, []profile.ProfileItem{profileItem})
}

func (D dbProvider) get(ctx context.Context, tier tier.Tier, profileKey profile.ProfileItemKey) (profile.ProfileItem, error) {
	ctx, t := timer.Start(ctx, tier.ID, "model.profile.db.get")
	defer t.Stop()
	profiles, err := D.getBatch(ctx, tier, []profile.ProfileItemKey{profileKey})
	if err != nil || len(profiles) == 0 {
		p := profile.NewProfileItem(string(profileKey.OType), profileKey.Oid, profileKey.Key, value.Nil, 0)
		return p, err
	}

	return profiles[0], nil

}

// getBatched returns the version for (otype, oid, key)
func (D dbProvider) getBatch(ctx context.Context, tier tier.Tier, profileKeys []profile.ProfileItemKey) ([]profile.ProfileItem, error) {
	ctx, t := timer.Start(ctx, tier.ID, "model.profile.db.getBatch")
	defer t.Stop()

	if len(profileKeys) == 0 {
		return []profile.ProfileItem{}, nil
	}

	// deduplicate profiles on (otype, oid, key)
	profileKeyUnqiue := make(map[profile.ProfileItemKey]struct{}, len(profileKeys))
	for _, vid := range profileKeys {
		if _, ok := profileKeyUnqiue[vid]; !ok {
			profileKeyUnqiue[vid] = struct{}{}
		}
	}

	// construct the select query and execute it
	sql := `
		SELECT otype, oid, zkey, value, version
		FROM profile
		WHERE (otype, oid, zkey) in 
	`
	v := make([]interface{}, 0, len(profileKeyUnqiue))
	inval := "("
	for pk := range profileKeyUnqiue {
		inval += "(?, ?, ?),"
		v = append(v, pk.OType, pk.Oid, pk.Key)
	}
	inval = strings.TrimSuffix(inval, ",") // remove the last trailing comma
	inval += ")"
	sql += inval
	profilereqs := make([]profileItemSer, 0)
	err := tier.DB.SelectContext(ctx, &profilereqs, sql, v...)
	if err != nil {
		return nil, err
	}
	ret := make([]profile.ProfileItem, 0, len(profileKeys))

	mapKeyToVal := make(map[profile.ProfileItemKey]profile.ProfileItem)

	if len(profilereqs) > len(profileKeys) {
		tier.Logger.Error("Found more than expected profiles in DB GetBatch", zap.Int("expected", len(profileKeys)), zap.Int("actual", len(profilereqs)))
	}

	for _, p := range profilereqs {
		prof, err := p.toProfileItem()
		if err == nil {
			prof.UpdateTime = 0
			mapKeyToVal[prof.GetProfileKey()] = prof
		}
	}

	for _, pk := range profileKeys {
		if val, ok := mapKeyToVal[pk]; ok {
			ret = append(ret, val)
		} else {
			ret = append(ret, profile.NewProfileItem(string(pk.OType), pk.Oid, pk.Key, value.Nil, 0))
		}
	}

	return ret, nil
}
