import * as pulumi from "@pulumi/pulumi";
import * as mysql from "@pulumi/mysql";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
  "managed-by": "fennel.ai",
}

export type inputType = {
  endpoint: string
  username: string
  password: pulumi.Output<string>
  db: string
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
  }, { provider });

  const output: outputType = {};
  return output;
};
