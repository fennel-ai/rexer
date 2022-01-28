package db

import (
	"database/sql"
	"fmt"
	"strings"
)

var defs map[uint32]string
var tablenames []string

func init() {
	// if you want to make any change to schema (create table, drop table, alter table etc.)
	// add a versioned query here. Numbers should be increasing with no gaps and no repetitions
	// Also, if  you create a create table query, also add the table name to "tablenames" list
	defs = map[uint32]string{
		1: `CREATE TABLE IF NOT EXISTS schema_version (
				version INT NOT NULL
			);`,
		2: `CREATE TABLE IF NOT EXISTS schema_test (
				zkey INT NOT NULL,
				value INT NOT NULL
			);`,
		3: `CREATE TABLE IF NOT EXISTS actionlog (
				action_id BIGINT not null primary key auto_increment,
				cust_id BIGINT not null,
				actor_id BIGINT NOT NULL,
				actor_type varchar(256) NULL,
				target_id BIGINT NOT NULL,
				target_type varchar(256) NOT NULL,
				action_type varchar(256) NOT NULL,
				action_value BIGINT NOT NULL,
				timestamp BIGINT NOT NULL,
				request_id BIGINT not null,
				INDEX (cust_id, action_id),
				INDEX (cust_id, action_value),
				INDEX (cust_id, timestamp)
		  );`,
		4: `CREATE TABLE IF NOT EXISTS checkpoint (
				cust_id BIGINT NOT NULL,
				counter_type INT NOT NULL,
				checkpoint BIGINT NOT NULL DEFAULT 0,
				PRIMARY KEY(cust_id, counter_type)
		  );`,
		5: `CREATE TABLE IF NOT EXISTS profile (
				cust_id BIGINT not null,
				otype varchar(256) not null,
				oid BIGINT not null,
				zkey varchar(256) not null,
				version BIGINT not null,
				value blob not null,
				PRIMARY KEY(cust_id, otype, oid, zkey, version)
		  );`,
		6: `CREATE TABLE IF NOT EXISTS counter_bucket (
				cust_id BIGINT NOT NULL,
				counter_type INT NOT NULL,
				window_type INT NOT NULL,
				idx BIGINT NOT NULL,
				count BIGINT NOT NULL DEFAULT 0,
				zkey varchar(256) NOT NULL,
				PRIMARY KEY(cust_id, counter_type, window_type, zkey, idx)
		  );`,
		7: `CREATE TABLE IF NOT EXISTS query_ast (
				query_id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
				cust_id BIGINT NOT NULL,
				timestamp BIGINT NOT NULL,
				query_ser BLOB NOT NULL,
				INDEX (cust_id, timestamp)
		  );`,
		8: `CREATE TABLE IF NOT EXISTS aggregate_config (
				cust_id BIGINT NOT NULL,
				aggregate_type VARCHAR(255) NOT NULL,
				name VARCHAR(255) NOT NULL,
				query_ser BLOB NOT NULL,
				timestamp BIGINT NOT NULL,
				options_ser BLOB NOT NULL,
				PRIMARY KEY(cust_id, aggregate_type, name)
			);`,
		9: `CREATE TABLE IF NOT EXISTS checkpoint2 (
				cust_id BIGINT NOT NULL,
				aggtype VARCHAR(255) NOT NULL,
				aggname VARCHAR(255) NOT NULL,
				checkpoint BIGINT NOT NULL DEFAULT 0,
				PRIMARY KEY(cust_id, aggtype, aggname)
		  );`,
	}
	tablenames = []string{
		"schema_version",
		"schema_test",
		"actionlog",
		"checkpoint",
		"profile",
		"counter_bucket",
		"query_ast",
		"aggregate_config",
		"checkpoint2",
	}

	if err := verifyDefs(); err != nil {
		panic(err)
	}
}

func verifyDefs() error {
	num_create_tables := 0
	for _, query := range defs {
		if strings.Contains(strings.ToLower(query), "create table") {
			num_create_tables += 1
		}
	}
	if num_create_tables != len(tablenames) {
		return fmt.Errorf("number of tables & create table queries not same in number - did you forget to keep them in sync?")
	}
	return nil
}

//Recreate_I_KNOW_WHAT_IM_DOING drops given database & creates another database with the same name
//and returns a connection to that. Currently, it is implemented by dropping all the tables one by one.
//This could lead to permanent data loss so, it should only be used in test instances - if you don't
//know what you're doing, you should not be using it
//TODO: instead of dropping table by table, use API to directly drop/create DB. When that is ready,
//we won't have to maintain 'tablenames' vairable anymore.
func Recreate_I_KNOW_WHAT_IM_DOING(db Connection) (Connection, error) {
	for _, name := range tablenames {
		_, err := db.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s;", name))
		if err != nil {
			return Connection{}, err
		}
	}
	return db, nil
}

func schemaVersion(db Connection) (uint32, error) {
	row := db.QueryRow("SELECT version FROM schema_version;")
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
	var err error
	if curr == 0 {
		_, err = db.Query("INSERT INTO schema_version VALUES (?);", 1)
	} else {
		_, err = db.Query("UPDATE schema_version SET version = version + 1;")
	}
	return err
}

func SyncSchema(db Connection) error {
	curr, err := schemaVersion(db)
	if err != nil {
		return err
	}
	len_ := uint32(len(defs))
	for i := curr + 1; i <= len_; i++ {
		query := defs[i]
		if _, err = db.Exec(query); err != nil {
			return err
		}
		if err = incrementSchemaVersion(db, i-1); err != nil {
			return err
		}
	}
	return nil
}
