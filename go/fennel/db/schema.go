package db

import (
	"database/sql"
	"fennel/lib/ftypes"
	"fmt"
	"strings"
)

var defs map[uint32]unnamedSQL
var tablenames []string

type unnamedSQL struct {
	sql       string
	tablename string
}

func init() {
	// if you want to make any change to schema (create table, drop table, alter table etc.)
	// add a versioned query here. Numbers should be increasing with no gaps and no repetitions
	// Also, if  you create a create table query, also add the table name to "tablenames" list
	defs = map[uint32]unnamedSQL{
		1: {`CREATE TABLE IF NOT EXISTS %s (
				version INT NOT NULL
			);`,
			"schema_version",
		},
		2: {`CREATE TABLE IF NOT EXISTS %s (
				zkey INT NOT NULL,
				value INT NOT NULL
			);`,
			"schema_test",
		},
		3: {`CREATE TABLE IF NOT EXISTS %s (
				action_id BIGINT not null primary key auto_increment,
				cust_id BIGINT not null,
				actor_id BIGINT NOT NULL,
				actor_type varchar(256) NOT NULL,
				target_id BIGINT NOT NULL,
				target_type varchar(256) NOT NULL,
				action_type varchar(256) NOT NULL,
				action_value BIGINT NOT NULL,
				timestamp BIGINT NOT NULL,
				request_id BIGINT not null,
				INDEX (cust_id, action_id),
				INDEX (cust_id, action_value),
				INDEX (cust_id, timestamp)
		 );`, "actionlog",
		},
		4: {`CREATE TABLE IF NOT EXISTS %s (
				cust_id BIGINT NOT NULL,
				aggtype VARCHAR(255) NOT NULL,
				aggname VARCHAR(255) NOT NULL,
				checkpoint BIGINT NOT NULL DEFAULT 0,
				PRIMARY KEY(cust_id, aggtype, aggname)
		 );`, "checkpoint",
		},
		5: {`CREATE TABLE IF NOT EXISTS %s (
				cust_id BIGINT not null,
				otype varchar(256) not null,
				oid BIGINT not null,
				zkey varchar(256) not null,
				version BIGINT not null,
				value blob not null,
				PRIMARY KEY(cust_id, otype, oid, zkey, version)
		 );`, "profile",
		},
		6: {`CREATE TABLE IF NOT EXISTS %s (
				cust_id BIGINT NOT NULL,
				counter_type INT NOT NULL,
				window_type INT NOT NULL,
				idx BIGINT NOT NULL,
				count BIGINT NOT NULL DEFAULT 0,
				zkey varchar(256) NOT NULL,
				PRIMARY KEY(cust_id, counter_type, window_type, zkey, idx)
		 );`, "counter_bucket",
		},
		7: {`CREATE TABLE IF NOT EXISTS %s (
				query_id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
				cust_id BIGINT NOT NULL,
				timestamp BIGINT NOT NULL,
				query_ser BLOB NOT NULL,
				INDEX (cust_id, timestamp)
		 );`, "query_ast",
		},
		8: {`CREATE TABLE IF NOT EXISTS %s (
				cust_id BIGINT NOT NULL,
				aggregate_type VARCHAR(255) NOT NULL,
				name VARCHAR(255) NOT NULL,
				query_ser BLOB NOT NULL,
				timestamp BIGINT NOT NULL,
				options_ser BLOB NOT NULL,
				PRIMARY KEY(cust_id, aggregate_type, name)
			);`, "aggregate_config",
		},
	}
	tablenames = []string{
		"schema_version",
		"schema_test",
		"actionlog",
		"profile",
		"counter_bucket",
		"query_ast",
		"aggregate_config",
		"checkpoint",
	}

	if err := verifyDefs(); err != nil {
		panic(err)
	}
}

// ToPlaneTablename returns the name of the given table in the context of a particular plane
func ToPlaneTablename(planeID ftypes.PlaneID, name string) (string, error) {
	if planeID == 0 {
		return "", fmt.Errorf("plane ID not initialized")
	}
	return fmt.Sprintf("plane_%d_%s", planeID, name), nil
}

func verifyDefs() error {
	num_create_tables := 0
	for _, query := range defs {
		if strings.Contains(strings.ToLower(query.sql), "create table") {
			num_create_tables += 1
		}
	}
	if num_create_tables != len(tablenames) {
		return fmt.Errorf("number of tables & create table queries not same in number - did you forget to keep them in sync?")
	}
	return nil
}

func schemaVersion(db Connection) (uint32, error) {
	if db.PlaneID() == 0 {
		return 0, fmt.Errorf("plane ID not initialized")
	}
	schemaTable, err := ToPlaneTablename(db.PlaneID(), "schema_version")
	if err != nil {
		return 0, err
	}
	row := db.QueryRow(fmt.Sprintf("SELECT version FROM %s", schemaTable))
	var total sql.NullInt32
	row.Scan(&total)
	if total.Valid {
		return uint32(total.Int32), nil
	} else {
		// this likely means that there was no such row and hence the table has not been initialized
		return 0, nil
	}
}

func incrementSchemaVersion(db Connection, curr uint32) error {
	if db.PlaneID() == 0 {
		return fmt.Errorf("plane ID not initialized")
	}
	var err error
	schemaTable, err := ToPlaneTablename(db.PlaneID(), "schema_version")
	if err != nil {
		return err
	}
	if curr == 0 {
		_, err = db.Query(fmt.Sprintf("INSERT INTO %s VALUES (?);", schemaTable), 1)
	} else {
		_, err = db.Query(fmt.Sprintf("UPDATE %s SET version = version + 1", schemaTable))
	}
	return err
}

func SyncSchema(db Connection) error {
	if db.PlaneID() == 0 {
		return fmt.Errorf("plane ID not initialized")
	}
	curr, err := schemaVersion(db)
	if err != nil {
		return err
	}
	len_ := uint32(len(defs))
	for i := curr + 1; i <= len_; i++ {
		planeName, err := ToPlaneTablename(db.PlaneID(), defs[i].tablename)
		if err != nil {
			return err
		}
		query := fmt.Sprintf(defs[i].sql, planeName)
		if _, err = db.Exec(query); err != nil {
			return err
		}
		if err = incrementSchemaVersion(db, i-1); err != nil {
			return err
		}
	}
	return nil
}
