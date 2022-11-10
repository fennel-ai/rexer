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
	// The relation b/w the tables are as follows.
	// A sagemaker_hosted_model will have several models associated with it.
	// model < sagemaker_hosted_model.
	// The model_id is the foreign key ( mapping to id ) in the model table.
	// The sagemaker_model_name refers to model_name in sagemaker_endpoint_config.
	// The name in the sagemaker_endpoint_config is the same as endpoint_config_name in sagemaker_endpoint.
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
			-- The following fields are optional and are only used for SageMaker Serverless Configs.
			serverless_max_concurrency INT NOT NULL DEFAULT 0,
			serverless_memory INT NOT NULL DEFAULT 0,
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

	// ================== BEGIN Schema for Phaser  ======================
	12: `CREATE TABLE IF NOT EXISTS phaser (
			namespace VARCHAR(64) NOT NULL,
			identifier VARCHAR(255) NOT NULL,
			s3_bucket VARCHAR(255) NOT NULL,
			s3_prefix VARCHAR(255) NOT NULL,
			phaser_schema ENUM('ITEM_SCORE_LIST', 'ITEM_LIST', 'STRING') NOT NULL,
			update_version BIGINT UNSIGNED DEFAULT 0,
			ttl BIGINT UNSIGNED DEFAULT 0,
			PRIMARY KEY (namespace, identifier)
		);`,
	// ==================== END Schema for Phaser ======================
	13: `ALTER TABLE aggregate_config ADD COLUMN source VARCHAR(64) NOT NULL DEFAULT 'action';`,
	// name in query_ast is meant to be unique, but it is not enforced by the schema.
	14: `ALTER TABLE query_ast ADD COLUMN name VARCHAR(64) NOT NULL;`,
	// Statement 15 adds the container_name column to the model table.
	// Statement 16 generates container names for rows with no container name.
	15: `ALTER TABLE model ADD COLUMN container_name VARCHAR(255) NOT NULL;`,
	16: `UPDATE model SET container_name=CONCAT("Container-", name, "-", version) WHERE container_name="";`,

	// ================== BEGIN Schema for Data Integration  ======================
	// Reserve 17 - 29 for different sources.
	17: `CREATE TABLE IF NOT EXISTS source (
			name VARCHAR(255) NOT NULL,
			type VARCHAR(255) NOT NULL,
			source_id VARCHAR(255) NOT NULL,
			last_updated timestamp default now() on update now(),
			PRIMARY KEY (name)
		);`,
	18: `CREATE TABLE IF NOT EXISTS connector (
			name VARCHAR(255) NOT NULL,
			version INT DEFAULT 0,
			source_name VARCHAR(255) NOT NULL,
			source_type VARCHAR(255) NOT NULL,
			stream_name VARCHAR(255) NOT NULL,
			destination VARCHAR(255) NOT NULL,
			query_ser BLOB NOT NULL,
			active BOOL NOT NULL DEFAULT TRUE,
			conn_id VARCHAR(255) NOT NULL,
			cursor_field VARCHAR(255) NOT NULL,
			last_updated timestamp default now() on update now(), 
			PRIMARY KEY (name, version),
			FOREIGN KEY (source_name) REFERENCES source(name) ON DELETE CASCADE
		);`,
	19: `CREATE TABLE IF NOT EXISTS s3_source (
			name VARCHAR(255) NOT NULL,
			bucket VARCHAR(255) NOT NULL,
			path_prefix VARCHAR(255) NOT NULL,
			format ENUM('csv','parquet', 'avro') NOT NULL,
			delimiter VARCHAR(1) NOT NULL DEFAULT ',',
			source_id VARCHAR(255) NOT NULL,
			json_schema VARCHAR(1024) NOT NULL,
			last_updated timestamp default now() on update now(), 
			PRIMARY KEY (name),
			FOREIGN KEY (name) REFERENCES source(name) ON DELETE CASCADE
			);`,
	20: `CREATE TABLE IF NOT EXISTS bigquery_source (
			name VARCHAR(255) NOT NULL,
			project_id VARCHAR(255) NOT NULL,
			dataset_id VARCHAR(255) NOT NULL,
			source_id VARCHAR(255) NOT NULL,
			last_updated timestamp default now() on update now(), 
			PRIMARY KEY (name),
			FOREIGN KEY (name) REFERENCES source(name) ON DELETE CASCADE
		);`,
	21: `CREATE TABLE IF NOT EXISTS postgres_source (
			name VARCHAR(255) NOT NULL,
			source_id VARCHAR(255) NOT NULL,
			db_name VARCHAR(255) NOT NULL,
			host VARCHAR(255) NOT NULL,
			jdbc_params VARCHAR(255) NOT NULL,
			port INT NOT NULL,
			last_updated timestamp default now() on update now(), 
			PRIMARY KEY (name),
			FOREIGN KEY (name) REFERENCES source(name) ON DELETE CASCADE
		);`,
	22: `CREATE TABLE IF NOT EXISTS mysql_source (
			name VARCHAR(255) NOT NULL,
			source_id VARCHAR(255) NOT NULL,
			db_name VARCHAR(255) NOT NULL,
			host VARCHAR(255) NOT NULL,
			port INT NOT NULL,
			jdbc_params VARCHAR(255) NOT NULL,
			last_updated timestamp default now() on update now(), 
			PRIMARY KEY (name),
			FOREIGN KEY (name) REFERENCES source(name) ON DELETE CASCADE
		);`,
	25: `CREATE TABLE IF NOT EXISTS snowflake_source (
			name VARCHAR(255) NOT NULL,	
			source_id VARCHAR(255) NOT NULL,
			db_name VARCHAR(255) NOT NULL,
			host VARCHAR(255) NOT NULL,
			jdbc_params VARCHAR(255) NOT NULL,
			role VARCHAR(255) NOT NULL,
			warehouse VARCHAR(255) NOT NULL,
			db_schema VARCHAR(255) NOT NULL,
			last_updated timestamp default now() on update now(), 
			PRIMARY KEY (name),
			FOREIGN KEY (name) REFERENCES source(name) ON DELETE CASCADE
		);`,
	// ==================== END Schema for Data Integration ======================
	// ==================== BEGIN Schema for usage counters ================
	23: `CREATE TABLE IF NOT EXISTS usage_counters (
		queries BIGINT NOT NULL,
		actions BIGINT NOT NULL,
		timestamp BIGINT NOT NULL,
		INDEX tx(timestamp)
	);`,
	// ==================== END Schema for usage counters ===================
	24: `ALTER TABLE aggregate_config ADD COLUMN mode VARCHAR(64) DEFAULT 'rql';`,
	26: `ALTER TABLE query_ast ADD COLUMN description VARCHAR(2048) DEFAULT '';`,
	27: `ALTER TABLE query_ast ADD INDEX (name);`,
}
