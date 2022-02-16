import * as pulumi from "@pulumi/pulumi";
import * as confluent from "@pulumi/confluent";

type input = {
    region: string,
    username: string,
    password: pulumi.Output<string>,
    envName: string,
}

const parseConfig = (): input => {
    const config = new pulumi.Config();
    return {
        username: config.require("username"),
        password: config.requireSecret("password"),
        region: config.require("region"),
        envName: config.require("env-name"),
    }
}

const config = parseConfig();

const provider = new confluent.Provider("conf-provider", {
    username: config.username,
    password: config.password,
})

const env = new confluent.ConfluentEnvironment("conf-env", {
    name: config.envName,
}, { provider })

export const cluster = new confluent.KafkaCluster("cluster", {
    availability: "LOW",
    environmentId: env.id,
    region: config.region,
    serviceProvider: "AWS",
}, { provider });
