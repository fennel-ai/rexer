package tier

import "fennel/db"

var Schema db.Schema

func init() {
	// if you want to make any change to Schema (create table, drop table, alter table etc.)
	// add a versioned query here. Numbers should be increasing with no gaps and no repetitions
	Schema = db.Schema{
		1: `CREATE TABLE IF NOT EXISTS actionlog (
				action_id BIGINT not null primary key auto_increment,
				actor_id BIGINT NOT NULL,
				actor_type varchar(255) NOT NULL,
				target_id BIGINT NOT NULL,
				target_type varchar(255) NOT NULL,
				action_type varchar(255) NOT NULL,
				timestamp BIGINT NOT NULL,
				request_id BIGINT not null,
				metadata BLOB NOT NULL,
				INDEX (timestamp)
		);`,
		2: `CREATE TABLE IF NOT EXISTS checkpoint (
				aggtype VARCHAR(255) NOT NULL,
				aggname VARCHAR(255) NOT NULL,
				checkpoint BIGINT NOT NULL DEFAULT 0,
				PRIMARY KEY(aggtype, aggname)
		);`,
		3: `CREATE TABLE IF NOT EXISTS profile (
				otype varchar(255) not null,
				oid BIGINT not null,
				zkey varchar(255) not null,
				version BIGINT not null,
				value blob not null,
				PRIMARY KEY(otype, oid, zkey, version)
		);`,
		4: `CREATE TABLE IF NOT EXISTS counter_bucket (
				counter_type INT NOT NULL,
				window_type INT NOT NULL,
				idx BIGINT NOT NULL,
				count BIGINT NOT NULL DEFAULT 0,
				zkey varchar(255) NOT NULL,
				PRIMARY KEY(counter_type, window_type, zkey, idx)
		);`,
		5: `CREATE TABLE IF NOT EXISTS query_ast (
				query_id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,
				timestamp BIGINT NOT NULL,
				query_ser BLOB NOT NULL,
				INDEX (timestamp)
		);`,
		6: `CREATE TABLE IF NOT EXISTS aggregate_config (
				name VARCHAR(255) NOT NULL,
				query_ser BLOB NOT NULL,
				timestamp BIGINT NOT NULL,
				options_ser BLOB NOT NULL,
				active BOOL NOT NULL DEFAULT TRUE,
				PRIMARY KEY(name),
				INDEX (active)
		);`,
	}
}
