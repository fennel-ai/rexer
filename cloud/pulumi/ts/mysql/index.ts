import * as pulumi from "@pulumi/pulumi";
import * as mysql from "@pulumi/mysql";

export type inputType = {
  endpoint: string
  username: string
  password: pulumi.Output<string>
  db: string
  protect: boolean
};

export const plugins = {
  "mysql": "v3.1.0",
}

export type outputType = {};

export const setup = (input: inputType) => {
  const { username, password, endpoint, db } = input;
  const provider = new mysql.Provider("mysql-provider", {
    endpoint,
    username,
    password,
  });

  const database = new mysql.Database("mysql-database", {
    name: db,
  }, { provider, protect: input.protect });

  const output: outputType = {};
  return output;
};
