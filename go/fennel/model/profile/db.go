package profile

import (
	"fennel/instance"
	"fennel/lib/ftypes"
	"fennel/lib/profile"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

//func set(this instance.Instance, otype uint32, oid uint64, key string, version uint64, valueSer []byte) error {
//}

//func get(this instance.Instance, otype uint32, oid uint64, key string, version uint64) ([]byte, error) {
//}

// we create a private interface to make testing caching easier
type provider interface {
	set(this instance.Instance, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error
	get(this instance.Instance, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error)
}

type dbProvider struct{}

func (D dbProvider) set(this instance.Instance, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64, valueSer []byte) error {
	if version == 0 {
		return fmt.Errorf("version can not be zero")
	}
	if len(key) > 256 {
		return fmt.Errorf("makeKey too long: keys can only be upto 256 chars")
	}
	if len(otype) > 256 {
		return fmt.Errorf("otype too long: otypes can only be upto 256 chars")
	}
	_, err := this.DB.Exec(`
		INSERT INTO profile 
			(cust_id, otype, oid, zkey, version, value)
		VALUES
			(?, ?, ?, ?, ?, ?);`,
		custid, otype, oid, key, version, valueSer)
	if err != nil {
		return err
	}
	return nil
	//return set(this, otype, oid, key, version, valueSer)
}

func (D dbProvider) get(this instance.Instance, custid ftypes.CustID, otype ftypes.OType, oid uint64, key string, version uint64) ([]byte, error) {
	var value [][]byte

	var err error
	if version > 0 {
		err = this.DB.Select(&value, `
		SELECT value
		FROM profile
		WHERE
			cust_id = ?
			AND otype = ?
			AND oid = ?
			AND zkey = ?
			AND version = ?
		LIMIT 1
		`, custid, otype, oid, key, version)
	} else {
		// if version isn't given, just pick the highest version
		err = this.DB.Select(&value, `
		SELECT value
		FROM profile
		WHERE
			cust_id = ?
			AND otype = ?
			AND oid = ?
			AND zkey = ?
		ORDER BY version DESC
		LIMIT 1
		`, custid, otype, oid, key)
	}
	if err != nil {
		return nil, err
	} else if len(value) == 0 {
		// i.e no valid value is found, so we return nil
		return nil, nil
	} else {
		return value[0], nil
	}
	//return get(this, otype, oid, key, version)
}

var _ provider = dbProvider{}

// Whatever properties of 'request' are non-zero are used to filter eligible profiles
func GetProfiles(this instance.Instance, request profile.ProfileFetchRequest) ([]profile.ProfileItemSer, error) {
	query := "SELECT * FROM profile"
	clauses := make([]string, 0)

	if request.CustID != 0 {
		clauses = append(clauses, "cust_id = :otype")
	}
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
	statement, err := this.DB.PrepareNamed(query)
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
