package main

import "fennel/db"

// if you want to make any change to Schema (create table, drop table, alter table etc.)
// add a versioned query here. Numbers should be increasing with no gaps and no repetitions
var Schema = db.Schema{
	1: `CREATE TABLE IF NOT EXISTS customer (
				customer_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				name VARCHAR(32) NOT NULL
		);`,
	2: `CREATE TABLE IF NOT EXISTS tier (
				tier_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				data_plane_id INT UNSIGNED NOT NULL,
				customer_id INT UNSIGNED NOT NULL,
				pulimi_stack VARCHAR(32) NOT NULL,
				api_url VARCHAR(256) NOT NULL UNIQUE,
				k8s_namespace VARCHAR(32) NOT NULL UNIQUE
		);`,
	3: `CREATE TABLE IF NOT EXISTS data_plane (
				data_plane_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				aws_role VARCHAR(64) NOT NULL,
				region VARCHAR(64) NOT NULL,
				pulimi_stack VARCHAR(32) NOT NULL UNIQUE,
				vpc_id VARCHAR(32) NOT NULL UNIQUE,
				eks_cluster_id VARCHAR(32) NOT NULL UNIQUE,
				kafka_instance_id INT UNSIGNED NOT NULL UNIQUE,
				db_instance_id INT UNSIGNED NOT NULL UNIQUE,
				redis_instance_id INT UNSIGNED NOT NULL UNIQUE,
				elasticache_instance_id INT UNSIGNED NOT NULL UNIQUE
		);`,
	4: `CREATE TABLE IF NOT EXISTS kafka (
				instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				confluent_environment VARCHAR(32) NOT NULL,
				confluent_cluster_id VARCHAR(32) NOT NULL,
				confluent_cluster_name VARCHAR(32) NOT NULL,
				kafka_bootstrap_servers VARCHAR(128) NOT NULL,
				kafka_api_key VARCHAR(128) NOT NULL,
				kafka_secret_key VARCHAR(128) NOT NULL
		);`,
	5: `CREATE TABLE IF NOT EXISTS db (
				instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				cluster_id VARCHAR(32) NOT NULL,
				cluster_security_group VARCHAR(32) NOT NULL,
				db_host VARCHAR(128) NOT NULL,
				admin_username VARCHAR(32) NOT NULL,
				admin_password VARCHAR(32) NOT NULL
		);`,
	6: `CREATE TABLE IF NOT EXISTS redis (
				instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				memory_db_cluster_id VARCHAR(32) NOT NULL,
				cluster_security_group VARCHAR(32) NOT NULL,
				hostname VARCHAR(128) NOT NULL
		);`,
	7: `CREATE TABLE IF NOT EXISTS elasticache (
				instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
				cluster_id VARCHAR(32) NOT NULL,
				cluster_security_group VARCHAR(32) NOT NULL,
				primary_hostname VARCHAR(128) NOT NULL,
				replica_hostname VARCHAR(128) NOT NULL
		);`,
}
