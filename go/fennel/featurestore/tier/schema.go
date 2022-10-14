package tier

import "fennel/db"

// if you want to make any change to Schema (create table, drop table, alter table etc.)
// add a versioned query here. Numbers should be increasing with no gaps and no repetitions
//
// NOTE: Queries here should be idempotent in nature i.e. the queries written here should
// take into consideration that they could be executed more than once
var Schema = db.Schema{
	1: `CREATE TABLE IF NOT EXISTS stream (
            name VARCHAR(255) NOT NULL,
            version INT DEFAULT 0,
            retention INT NOT NULL,
            start INT NOT NULL,
			stream_schema BLOB NOT NULL,
            last_updated timestamp default now() on update now(),
            PRIMARY KEY (name, version)
        );`,
	2: `CREATE TABLE IF NOT EXISTS source (
            name VARCHAR(255) NOT NULL,
            type VARCHAR(255) NOT NULL,
            source_id VARCHAR(255) NOT NULL,
            last_updated timestamp default now() on update now(),
            PRIMARY KEY (name)
        );`,
	3: `CREATE TABLE IF NOT EXISTS connector (
            name VARCHAR(255) NOT NULL,
            source_name VARCHAR(255) NOT NULL,
            source_type VARCHAR(255) NOT NULL,
            stream_name VARCHAR(255) NOT NULL,
            function BLOB NOT NULL,
            conn_id VARCHAR(255) NOT NULL,
            cursor_field VARCHAR(255) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            last_updated timestamp default now() on update now(),
            PRIMARY KEY (name),
            FOREIGN KEY (source_name) REFERENCES source(name) ON DELETE CASCADE
        );`,
	4: `CREATE TABLE IF NOT EXISTS s3_source (
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
	5: `CREATE TABLE IF NOT EXISTS bigquery_source (
            name VARCHAR(255) NOT NULL,
            project_id VARCHAR(255) NOT NULL,
            dataset_id VARCHAR(255) NOT NULL,
            source_id VARCHAR(255) NOT NULL,
            last_updated timestamp default now() on update now(), 
            PRIMARY KEY (name),
            FOREIGN KEY (name) REFERENCES source(name) ON DELETE CASCADE
        );`,
	6: `CREATE TABLE IF NOT EXISTS postgres_source (
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
	7: `CREATE TABLE IF NOT EXISTS mysql_source (
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
}
