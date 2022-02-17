import * as pulumi from "@pulumi/pulumi";

import { nameof } from "../lib/util";

import process = require('process');

export const plugins = {
}

export type inputType = {
}

export type outputType = {
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {}
}

export const setup = (input: inputType) => {
    const output: outputType = {}
    return output
}

let output;
// Run the main function only if this program is run through the pulumi CLI.
// Unfortunately, in that case the argv0 itself is not "pulumi", but the full
// path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
if (process.argv0 !== 'node') {
    pulumi.log.info("Running...")
    const input = parseConfig();
    output = setup(input)
}
export { output };