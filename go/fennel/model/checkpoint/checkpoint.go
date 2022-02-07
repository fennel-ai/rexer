package checkpoint

import (
	"database/sql"
	"fennel/lib/ftypes"
	"fennel/tier"
)

func Get(tier tier.Tier, aggtype ftypes.AggType, aggname ftypes.AggName) (ftypes.OidType, error) {
	row := tier.DB.QueryRow(`
		SELECT checkpoint
		FROM checkpoint 
		WHERE aggtype = ?
		  AND aggname = ?;
	`, aggtype, aggname,
	)
	var checkpoint uint64
	err := row.Scan(&checkpoint)
	if err != nil && err != sql.ErrNoRows {
		return 0, err
	} else if err == sql.ErrNoRows {
		// this happens when no matching row was found. By default, checkpoint is zero
		return 0, nil
	} else {
		return ftypes.OidType(checkpoint), nil
	}
}

func Set(tier tier.Tier, aggtype ftypes.AggType, aggname ftypes.AggName, checkpoint ftypes.OidType) error {
	_, err := tier.DB.Exec(`
		INSERT INTO checkpoint (aggtype, aggname, checkpoint)
        VALUES (?, ?, ?)
		ON DUPLICATE KEY
		UPDATE
			checkpoint = ?
		;`,
		aggtype, aggname, checkpoint, checkpoint,
	)
	return err
}
