import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import * as kafkatopics from "../kafkatopics";
import { nameof } from "../lib/util"

import process = require('process')

export const plugins = {
    "kafka": "v3.1.2",
    "confluent": "v0.2.2"
}

// This is our pulumi program in "inline function" form
const setupResources = async () => {
    const config = new pulumi.Config()
    // setup kakfa topics.
    const kafkaTopicOutput = kafkatopics.setup({
        username: process.env.CONFLUENT_CLOUD_USERNAME,
        password: pulumi.output(process.env.CONFLUENT_CLOUD_PASSWORD),
        topicNames: config.requireObject("topicNames"),
        kafkaCluster: config.requireObject("kafkaCluster"),
    })
};

export type tierConfig = {
    tierId: number
    kafkaCluster: kafkatopics.cluster,
    topicNames: [string],
}

export const setupTier = async (config: tierConfig, destroy?: boolean) => {
    const projectName = `arkay`
    const stackName = `fennel/${projectName}/tier-${config.tierId}`

    console.info("initializing stack");
    // Create our stack 
    const args: InlineProgramArgs = {
        projectName,
        stackName,
        program: setupResources,
    };
    // create (or select if one already exists) a stack that uses our inline program
    const stack = await LocalWorkspace.createOrSelectStack(args);
    console.info("successfully initialized stack");

    console.info("installing plugins...");
    Object.keys(plugins).forEach(async (key) => {
        await stack.workspace.installPlugin(key, plugins[key])
    })
    console.info("plugins installed");

    console.info("setting up config");
    // TODO: Get these as input arguments to the function.
    await stack.setConfig(nameof<kafkatopics.inputType>("topicNames"), { value: JSON.stringify(config.topicNames) })
    await stack.setConfig(nameof<kafkatopics.inputType>("kafkaCluster"), { value: JSON.stringify(config.kafkaCluster) })
    console.info("config set");

    console.info("refreshing stack...");
    await stack.refresh({ onOutput: console.info });
    console.info("refresh complete");

    if (destroy) {
        console.info("destroying stack...");
        await stack.destroy({ onOutput: console.info });
        console.info("stack destroy complete");
        process.exit(0);
    }

    console.info("updating stack...");
    const upRes = await stack.up({ onOutput: console.info });
    console.log(upRes)
};

export default setupTier