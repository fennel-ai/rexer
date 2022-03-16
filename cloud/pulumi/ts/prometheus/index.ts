import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as process from "process";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "aws": "v4.38.0"
}

export type inputType = {
    region: string,
    roleArn: string,
    planeId: number,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    arn: string,
    prometheusWriteEndpoint: string,
    prometheusQueryEndpoint: string,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),
    }
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const awsProvider = new aws.Provider("prom-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const workspaceName = `fennel-prom-${input.planeId}`
    const prom = new aws.amp.Workspace(workspaceName, {
        alias: workspaceName,
    }, {provider: awsProvider})

    const arn = prom.arn
    const prometheusWriteEndpoint = prom.prometheusEndpoint.apply(endpoint => {
        // endpoint ends with `/`.
        return `${endpoint}api/v1/remote_write`
    }) 
    const prometheusQueryEndpoint = prom.prometheusEndpoint.apply(endpoint => {
        // endpoint ends with `/`.
        return `${endpoint}api/v1/query`
    })

    const output = pulumi.output({
        arn,
        prometheusWriteEndpoint,
        prometheusQueryEndpoint,
    })
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