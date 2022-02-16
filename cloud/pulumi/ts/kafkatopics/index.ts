import * as confluent from "@pulumi/confluent";
import * as kafka from "@pulumi/kafka";
import * as pulumi from "@pulumi/pulumi";
import process = require('process');

// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
const nameof = <T>(name: keyof T) => name;

export type cluster = {
    environmentId: string,
    id: string,
    bootstrapServers: string,
}

export type inputType = {
    username: string,
    password: pulumi.Output<string>
    topicNames: [string],
    kafkaCluster: cluster
}

export type outputType = {
    topics: kafka.Topic[]
}

// We have parseConfig as a standard function across all components because
// we do not want to call config.require inside of setup since the config parameters
// could come from either config.require or from parameters passed in by calling setup
// in another file.
const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        username: config.require(nameof<inputType>("username")),
        password: config.requireSecret(nameof<inputType>("password")),
        topicNames: config.requireObject(nameof<inputType>("topicNames")),
        kafkaCluster: config.requireObject(nameof<inputType>("kafkaCluster"))
    }
}

export const setup = (input: inputType) => {
    const confluentProvider = new confluent.Provider("conf-provider", {
        username: input.username,
        password: input.password,
    })

    const apiKey = new confluent.ApiKey("key", {
        environmentId: input.kafkaCluster.environmentId,
        clusterId: input.kafkaCluster.id,
    }, { provider: confluentProvider })

    const kafkaProvider = new kafka.Provider("kafka-provider", {
        bootstrapServers: [input.kafkaCluster.bootstrapServers.substring(input.kafkaCluster.bootstrapServers.lastIndexOf('/') + 1)],
        saslUsername: apiKey.key,
        saslPassword: apiKey.secret,
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