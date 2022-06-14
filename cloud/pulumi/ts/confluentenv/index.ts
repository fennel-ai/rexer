import * as pulumi from "@pulumi/pulumi";
import * as confluent from "@pulumi/confluent";

export const plugins = {
    "confluent": "v0.2.2",
}

export type inputType = {
    region: string,
    username: string,
    password: pulumi.Output<string>,
    envName: string,
    protect: boolean,
}

export type outputType = {
    bootstrapServer: string,
    apiKey: string,
    apiSecret: string,
    environmentId: string,
    clusterId: string,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new confluent.Provider("conf-provider", {
        username: input.username,
        password: input.password,
    })

    const env = new confluent.ConfluentEnvironment("conf-env", {
        name: input.envName,
    }, { provider, protect: input.protect })

    const cluster = new confluent.KafkaCluster("cluster", {
        availability: "LOW",
        environmentId: env.id,
        region: input.region,
        serviceProvider: "AWS",
    }, { provider, protect: input.protect });

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
