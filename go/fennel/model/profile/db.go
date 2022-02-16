package profile

import (
	"context"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fennel/lib/timer"
	"fennel/tier"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

// we create a private interface to make testing caching easier
type provider interface {
	set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error
	get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error)
}

type dbProvider struct{}

func (D dbProvider) set(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	defer timer.Start(tier.ID, "model.profile.db.set").ObserveDuration()
	if version == 0 {
		return fmt.Errorf("version can not be zero")
	}
	if len(key) > 255 {
		return fmt.Errorf("makeKey too long: keys can only be upto 255 chars")
	}
	if len(otype) > 255 {
		return fmt.Errorf("otype too long: otypes can only be upto 255 chars")
	}
	_, err := tier.DB.ExecContext(ctx, `
		INSERT INTO profile 
			(otype, oid, zkey, version, value)
		VALUES
			(?, ?, ?, ?, ?);`,
		otype, oid, key, version, valueSer,
	)
	return err
}

func (D dbProvider) get(ctx context.Context, tier tier.Tier, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	defer timer.Start(tier.ID, "model.profile.db.get").ObserveDuration()
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
