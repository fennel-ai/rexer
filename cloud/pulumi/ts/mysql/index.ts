import * as pulumi from "@pulumi/pulumi";
import * as mysql from "@pulumi/mysql";

import { nameof } from "../lib/util";

import process = require("process");

export type inputType = {
  username: string;
  password: pulumi.Output<string>;
  endpoint: string;
};

export type outputType = {
  database: mysql.Database;
};

const parseConfig = (): inputType => {
  const config = new pulumi.Config();
  return {
    username: config.require(nameof<inputType>("username")),
    password: config.requireSecret(nameof<inputType>("password")),
    endpoint: config.require(nameof<inputType>("endpoint")),
  };
};

export const setup = (input: inputType) => {
  const { username, password, endpoint } = input;
  const provider = new mysql.Provider("mysql-provider", {
    endpoint,
    username,
    password,
  });

  const database = new mysql.Database("mysql-database", {}, { provider });

  const output: outputType = {
    database,
  };
  return output;
};

let output;
// Run the main function only if this program is run through the pulumi CLI.
// Unfortunately, in that case the argv0 itself is not "pulumi", but the full
// path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
if (process.argv0 !== "node") {
  pulumi.log.info("Running...");
  const input = parseConfig();
  output = setup(input);
}
export { output };
