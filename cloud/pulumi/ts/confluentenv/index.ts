import * as pulumi from "@pulumi/pulumi";
import * as confluent from "@pulumi/confluent";

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
    "confluent": "v0.2.2",
}

export type inputType = {
    region: string,
    username: string,
    password: pulumi.Output<string>,
    envName: string,
}

export type outputType = {
    bootstrapServer: string,
    apiKey: string,
    apiSecret: string,
    environmentId: string,
    clusterId: string,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        username: config.require(nameof<inputType>("username")),
        password: config.requireSecret(nameof<inputType>("password")),
        region: config.require(nameof<inputType>("region")),
        envName: config.require(nameof<inputType>("envName")),
    }
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new confluent.Provider("conf-provider", {
        username: input.username,
        password: input.password,
    })

    const env = new confluent.ConfluentEnvironment("conf-env", {
        name: input.envName,
    }, { provider })

    const cluster = new confluent.KafkaCluster("cluster", {
        availability: "LOW",
        environmentId: env.id,
        region: input.region,
        serviceProvider: "AWS",
    }, { provider });

    const apiKey = new confluent.ApiKey("key", {
        environmentId: cluster.environmentId,
        clusterId: cluster.id,
    }, { provider })

    const output = pulumi.output({
        bootstrapServer: cluster.bootstrapServers.apply(server => server.substring(server.indexOf(":") + 3)),
        apiKey: apiKey.key,
        apiSecret: apiKey.secret,
        environmentId: cluster.environmentId,
        clusterId: cluster.id,
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