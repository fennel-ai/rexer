import * as pulumi from "@pulumi/pulumi";
import * as kafka from "@pulumi/kafka";
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
    "kafka": "v3.1.2",
}

export type inputType = {
    apiKey: string,
    apiSecret: pulumi.Output<string>
    topicNames: string[],
    bootstrapServer: string,
}

export type outputType = {
    topics: kafka.Topic[]
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        apiKey: config.require(nameof<inputType>("apiKey")),
        apiSecret: config.requireSecret(nameof<inputType>("apiSecret")),
        topicNames: config.requireObject(nameof<inputType>("topicNames")),
        bootstrapServer: config.require(nameof<inputType>("bootstrapServer"))
    }
}

export const setup = async (input: inputType) => {
    const kafkaProvider = new kafka.Provider("kafka-provider", {
        bootstrapServers: [input.bootstrapServer],
        saslUsername: input.apiKey,
        saslPassword: input.apiSecret,
        saslMechanism: "plain",
        tlsEnabled: true,
    })

    const topics = input.topicNames.map((topicName) => {
        return new kafka.Topic(`topic-${topicName}`, {
            name: topicName,
            partitions: 1,
            // We set replication factor to 3 regardless of the cluster availability
            // since that's the minimum required by confluent cloud:
            // https://github.com/Mongey/terraform-provider-kafka/issues/40#issuecomment-456897983
            replicationFactor: 3,
        }, { provider: kafkaProvider })
    })

    const output: outputType = {
        topics
    }
    return output
}

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