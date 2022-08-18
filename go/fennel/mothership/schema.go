package mothership

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
                pulumi_stack VARCHAR(128) NOT NULL UNIQUE,
                api_url VARCHAR(256) NOT NULL UNIQUE,
                k8s_namespace VARCHAR(32) NOT NULL UNIQUE
        );`,
	3: `CREATE TABLE IF NOT EXISTS data_plane (
                data_plane_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                aws_role VARCHAR(128) NOT NULL,
                region VARCHAR(16) NOT NULL,
                pulumi_stack VARCHAR(128) NOT NULL UNIQUE,
                vpc_id VARCHAR(32) NOT NULL UNIQUE,
                eks_instance_id INT UNSIGNED NOT NULL UNIQUE,
                kafka_instance_id INT UNSIGNED NOT NULL UNIQUE,
                db_instance_id INT UNSIGNED NOT NULL UNIQUE,
                memory_db_instance_id INT UNSIGNED NOT NULL UNIQUE,
                elasticache_instance_id INT UNSIGNED NOT NULL UNIQUE
        );`,
	4: `CREATE TABLE IF NOT EXISTS eks (
                instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                cluster_id VARCHAR(64) NOT NULL,
                min_instances INT UNSIGNED NOT NULL,
                max_instances INT UNSIGNED NOT NULL,
                instance_type VARCHAR(32) NOT NULL
        );`,
	5: `CREATE TABLE IF NOT EXISTS kafka (
                instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                confluent_environment VARCHAR(32) NOT NULL,
                confluent_cluster_id VARCHAR(32) NOT NULL,
                confluent_cluster_name VARCHAR(32) NOT NULL,
                kafka_bootstrap_servers VARCHAR(128) NOT NULL,
                kafka_api_key VARCHAR(128) NOT NULL,
                kafka_secret_key VARCHAR(128) NOT NULL
        );`,
	6: `CREATE TABLE IF NOT EXISTS db (
                instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                cluster_id VARCHAR(32) NOT NULL,
                cluster_security_group VARCHAR(32) NOT NULL,
                db_host VARCHAR(128) NOT NULL,
                admin_username VARCHAR(32) NOT NULL,
                admin_password VARCHAR(32) NOT NULL
        );`,
	7: `CREATE TABLE IF NOT EXISTS memory_db (
                instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                cluster_id VARCHAR(32) NOT NULL,
                cluster_security_group VARCHAR(32) NOT NULL,
                hostname VARCHAR(128) NOT NULL
        );`,
	8: `CREATE TABLE IF NOT EXISTS elasticache (
                instance_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                cluster_id VARCHAR(32) NOT NULL,
                cluster_security_group VARCHAR(32) NOT NULL,
                primary_hostname VARCHAR(128) NOT NULL,
                replica_hostname VARCHAR(128) NOT NULL
        );`,
	9: `CREATE TABLE IF NOT EXISTS launch_request (
                launch_request_id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                config JSON NOT NULL,
                status JSON NOT NULL
        );`,
	10: `CREATE TABLE IF NOT EXISTS launch_history (
                launch_request_id INT UNSIGNED NOT NULL PRIMARY KEY,
                config JSON NOT NULL,
                status JSON NOT NULL
        );`,
	11: `CREATE TABLE IF NOT EXISTS user (
                id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
                email VARCHAR(255) NOT NULL,
                encrypted_password VARBINARY(255) NOT NULL,

                remember_token VARCHAR(255),
                remember_created_at BIGINT UNSIGNED,
                confirmation_token VARCHAR(255),
                confirmation_sent_at BIGINT UNSIGNED,
                confirmed_at BIGINT UNSIGNED,
                reset_token VARCHAR(255),
                reset_sent_at BIGINT UNSIGNED,

                customer_id INT UNSIGNED NOT NULL,

                deleted_at BIGINT UNSIGNED NOT NULL,
                created_at BIGINT UNSIGNED NOT NULL,
                updated_at BIGINT UNSIGNED NOT NULL,

                UNIQUE KEY (email),
                UNIQUE KEY (remember_token),
                UNIQUE KEY (reset_token),
                UNIQUE KEY (confirmation_token)
        );`,
	12: `ALTER TABLE customer
                ADD COLUMN domain VARCHAR(255) UNIQUE,
                ADD COLUMN deleted_at BIGINT UNSIGNED NOT NULL,
                ADD COLUMN created_at BIGINT UNSIGNED NOT NULL,
                ADD COLUMN updated_at BIGINT UNSIGNED NOT NULL;
        `,
	13: `ALTER TABLE tier
                ADD COLUMN deleted_at BIGINT UNSIGNED NOT NULL,
                ADD COLUMN created_at BIGINT UNSIGNED NOT NULL,
                ADD COLUMN updated_at BIGINT UNSIGNED NOT NULL;
        `,
	14: `ALTER TABLE data_plane
                ADD COLUMN deleted_at BIGINT UNSIGNED NOT NULL,
                ADD COLUMN created_at BIGINT UNSIGNED NOT NULL,
                ADD COLUMN updated_at BIGINT UNSIGNED NOT NULL;
        `,
}
