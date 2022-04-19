import * as pulumi from "@pulumi/pulumi";
import * as process from "process";

export const plugins = {}

export type inputType = {}

// should not contain any pulumi.Output<> types.
export type outputType = {}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {}
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const output = pulumi.output({})
    return output
}

async function run() {
    let output: pulumi.Output<outputType> | undefined;
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