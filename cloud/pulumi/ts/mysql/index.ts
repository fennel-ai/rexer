import * as pulumi from "@pulumi/pulumi";
import * as mysql from "@pulumi/mysql";
import * as process from "process";

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

const parseConfig = (): inputType => {
  const config = new pulumi.Config();
  return {
    endpoint: config.require(nameof<inputType>("endpoint")),
    username: config.require(nameof<inputType>("username")),
    password: config.requireSecret(nameof<inputType>("password")),
    db: config.require(nameof<inputType>("db")),
  };
};

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

async function run() {
  let output: outputType | undefined;
  // Run the main function only if this program is run through the pulumi CLI.
  // Unfortunately, in that case the argv0 itself is not "pulumi", but the full
  // path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
  if (process.argv0 !== 'node') {
    pulumi.log.info("Running...")
    const input: inputType = parseConfig();
    output = await setup(input)
  }
  return output
}


export const output = await run();