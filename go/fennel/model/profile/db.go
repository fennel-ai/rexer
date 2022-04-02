package profile

import (
	"context"
	"fmt"
	"strings"

	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/tier"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// we create a private interface to make testing caching easier
type provider interface {
	set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error
	setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItemSer) error
	get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error)
	getVersionBatched(ctx context.Context, tier tier.Tier, vids []versionIdentifier) (map[versionIdentifier]uint64, error)
}

type dbProvider struct{}

type versionIdentifier struct {
	otype ftypes.OType
	oid   uint64
	key   string
}

func (D dbProvider) setBatch(ctx context.Context, tier tier.Tier, profiles []profile.ProfileItemSer) error {
	defer timer.Start(ctx, tier.ID, "model.profile.db.setBatch").Stop()
	if len(profiles) == 0 {
		return nil
	}
	// validate profiles
	for _, profile := range profiles {
		if profile.Version == 0 {
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
	for _, profile := range profiles {
		sql += "(?, ?, ?, ?, ?),"
		vals = append(vals, profile.OType, profile.Oid, profile.Key, profile.Version, profile.Value)
	}
	sql = strings.TrimSuffix(sql, ",") // remove the last trailing comma

	// this is to simulate `insert if not exists` semantics in SQL by setting value to itself
	// in case of INSERT returns a duplicate key error
	//
	// NOTE: This may result in unexpected behavior is the user tries to set a different
	// value for a versioned profile. Previously we had adopted failing such requests, but to
	// make the behavior idempotent, we do not fail the request but retain the value as is.
	// We expect the user to not set the version in case of such updates.
	//
	// NOTE: any AUTO_INCREMENT columns are incremented if UPDATE path is triggered
	sql += " ON DUPLICATE KEY UPDATE value=value"

	_, err := tier.DB.ExecContext(ctx, sql, vals...)
	return err
}

func (D dbProvider) set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	defer timer.Start(ctx, tier.ID, "model.profile.db.set").Stop()
	return D.setBatch(ctx, tier, []profile.ProfileItemSer{profile.NewProfileItemSer(string(otype), oid, key, version, valueSer)})
}

func (D dbProvider) get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	defer timer.Start(ctx, tier.ID, "model.profile.db.get").Stop()
	var value [][]byte = nil
	var err error
	if version > 0 {
		err = tier.DB.SelectContext(ctx, &value, `
		SELECT value
		FROM profile 
		WHERE
			otype = ?
			AND oid = ?
			AND zkey = ?
			AND version = ?
		LIMIT 1
		`, otype, oid, key, version,
		)
	} else {
		// if version isn't given, just pick the highest version
		err = tier.DB.SelectContext(ctx, &value, `
		SELECT value
		FROM profile 
		WHERE
			otype = ?
			AND oid = ?
			AND zkey = ?
		ORDER BY version DESC
		LIMIT 1
		`, otype, oid, key,
		)
	}
	if err != nil {
		return nil, err
	} else if len(value) == 0 {
		// i.e no valid value is found, so we return nil
		return nil, nil
	} else {
		return value[0], nil
	}
}

// getVersionBatched returns the largest version of the profile identified using (otype, oid, key)
func (D dbProvider) getVersionBatched(ctx context.Context, tier tier.Tier, vids []versionIdentifier) (map[versionIdentifier]uint64, error) {
	// deduplicate profiles on (otype, oid, key)
	vidUnique := make(map[versionIdentifier]struct{})
	for _, vid := range vids {
		if _, ok := vidUnique[vid]; !ok {
			vidUnique[vid] = struct{}{}
		}
	}

	// construct the select query and execute it
	sql := `
		SELECT otype, oid, zkey, max(version) as version
		FROM profile
		WHERE (otype, oid, zkey) in 
	`
	v := make([]interface{}, 0)
	inval := "("
	for vid, _ := range vidUnique {
		inval += "(?, ?, ?),"
		v = append(v, vid.otype, vid.oid, vid.key)
	}
	inval = strings.TrimSuffix(inval, ",") // remove the last trailing comma
	inval += ")"
	sql += inval
	sql += " GROUP BY otype, oid, zkey"

	profilereqs := make([]profile.ProfileFetchRequest, 0)
	err := tier.DB.SelectContext(ctx, &profilereqs, sql, v...)
	if err != nil {
		return nil, err
	}

	versionByVid := make(map[versionIdentifier]uint64)
	for _, p := range profilereqs {
		versionByVid[versionIdentifier{p.OType, p.Oid, p.Key}] = p.Version
	}
	return versionByVid, nil
}

var _ provider = dbProvider{}

// Whatever properties of 'request' are non-zero are used to filter eligible profiles
func GetMulti(ctx context.Context, tier tier.Tier, request profile.ProfileFetchRequest) ([]profile.ProfileItemSer, error) {
	query := "SELECT * FROM profile"
	clauses := make([]string, 0)

	if len(request.OType) != 0 {
		clauses = append(clauses, "otype = :otype")
	}
	if request.Oid != 0 {
		clauses = append(clauses, "oid = :oid")
	}
	if len(request.Key) != 0 {
		clauses = append(clauses, "zkey = :zkey")
	}
	if request.Version != 0 {
		clauses = append(clauses, "version = :version")
	}

	if len(clauses) > 0 {
		query = fmt.Sprintf("%s WHERE %s", query, strings.Join(clauses, " AND "))
	}
	query = fmt.Sprintf("%s LIMIT 1000;", query)
	profiles := make([]profile.ProfileItemSer, 0)
	statement, err := tier.DB.PrepareNamedContext(ctx, query)
	if err != nil {
		return nil, err
	}
	err = statement.Select(&profiles, request)
	if err != nil {
		return nil, err
	} else {
		return profiles, nil
	}
}
