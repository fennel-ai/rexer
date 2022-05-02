package tier

import "fennel/db"

// if you want to make any change to Schema (create table, drop table, alter table etc.)
// add a versioned query here. Numbers should be increasing with no gaps and no repetitions
//
// NOTE: Queries here should be idempotent in nature i.e. the queries written here should
// take into consideration that they could be executed more than once
var Schema = db.Schema{
	1: `CREATE TABLE IF NOT EXISTS actionlog (
			action_id BIGINT UNSIGNED not null primary key auto_increment,
			actor_id VARCHAR(64) NOT NULL,
			actor_type varchar(255) NOT NULL,
			target_id VARCHAR(64) NOT NULL,
			target_type varchar(255) NOT NULL,
			action_type varchar(255) NOT NULL,
			timestamp BIGINT UNSIGNED NOT NULL,
			request_id VARCHAR(64) NOT NULL,
			metadata BLOB NOT NULL,
			INDEX (timestamp)
		);`,
	2: `CREATE TABLE IF NOT EXISTS checkpoint (
			aggtype VARCHAR(255) NOT NULL,
			aggname VARCHAR(255) NOT NULL,
			checkpoint BIGINT UNSIGNED NOT NULL DEFAULT 0,
			PRIMARY KEY(aggtype, aggname)
		);`,
	3: `CREATE TABLE IF NOT EXISTS profile (
			otype varchar(255) not null,
			oid VARCHAR(64) NOT NULL,
			zkey varchar(255) not null,
			version BIGINT UNSIGNED not null,
			value blob not null,
			PRIMARY KEY(otype, oid, zkey)
		);`,
	4: `CREATE TABLE IF NOT EXISTS counter_bucket (
			counter_type INT NOT NULL,
			window_type INT NOT NULL,
			idx BIGINT UNSIGNED NOT NULL,
			count BIGINT UNSIGNED NOT NULL DEFAULT 0,
			zkey varchar(255) NOT NULL,
			PRIMARY KEY(counter_type, window_type, zkey, idx)
		);`,
	5: `CREATE TABLE IF NOT EXISTS query_ast (
			query_id BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
			timestamp BIGINT UNSIGNED NOT NULL,
			query_ser BLOB NOT NULL,
			INDEX (timestamp)
		);`,
	6: `CREATE TABLE IF NOT EXISTS aggregate_config (
			name VARCHAR(255) NOT NULL,
			query_ser BLOB NOT NULL,
			timestamp BIGINT UNSIGNED NOT NULL,
			options_ser BLOB NOT NULL,
			active BOOL NOT NULL DEFAULT TRUE,
			PRIMARY KEY(name),
			INDEX (active)
		);`,
	// ================== BEGIN Schema for model registry ======================
	7: `CREATE TABLE IF NOT EXISTS model (
			id BIGINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(255) NOT NULL,
			version VARCHAR(255) NOT NULL,
			framework VARCHAR(255) NOT NULL,
			framework_version VARCHAR(64) NOT NULL,
			artifact_path VARCHAR(255) NOT NULL,
			last_modified BIGINT UNSIGNED NOT NULL,
			active BOOL NOT NULL DEFAULT TRUE,
			UNIQUE KEY (name, version)
		);`,

	8: `CREATE TABLE IF NOT EXISTS sagemaker_hosted_model (
			sagemaker_model_name VARCHAR(255) NOT NULL,
			model_id BIGINT UNSIGNED NOT NULL,
			container_hostname VARCHAR(255) NOT NULL,
			PRIMARY KEY (sagemaker_model_name, model_id)
		);`,

	9: `CREATE TABLE IF NOT EXISTS sagemaker_endpoint_config (
			name VARCHAR(255) NOT NULL,
			variant_name VARCHAR(255) NOT NULL,
			model_name VARCHAR(255) NOT NULL,
			instance_type VARCHAR(255) NOT NULL,
			instance_count INT NOT NULL DEFAULT 1,
			PRIMARY KEY (name, variant_name)
		);`,

	10: `CREATE TABLE IF NOT EXISTS sagemaker_endpoint (
			name VARCHAR(255) NOT NULL,
			endpoint_config_name VARCHAR(255) NOT NULL,
			active bool NOT NULL DEFAULT true,
			PRIMARY KEY (name, endpoint_config_name)
		);`,
	// ==================== END Schema for model registry ======================

	11: `ALTER TABLE aggregate_config ADD COLUMN id INT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE;`,
	12: `ALTER TABLE aggregate_config ADD COLUMN update_version BIGINT UNSIGNED NOT NULL;`,
}
