import * as pulumi from "@pulumi/pulumi";
import * as mysql from "@pulumi/mysql";
import * as kafka from "@pulumi/kafka";

const config = new pulumi.Config();

const host = config.require("host");
const username = config.require("username");
const password = config.require("password");
const port = config.require("port");

// endpoint (required parameter for provider) - The address of the MySQL server to use. Most often a "hostname:port"
const endpoint = `${host}:${port}`;
const provider = new mysql.Provider("mysql-provider", {
  endpoint,
  username,
  password,
});

const database = new mysql.Database("mysql-database", {}, { provider });

const partitions = 1;
const replicationFactor = config.getNumber("replicationFactor") || 2;

const logs = new kafka.Topic("kafka-logs", {
  partitions,
  replicationFactor,
});

// Export the name of the bucket
export const databaseId = database.id;
export const logsId = logs.id;
