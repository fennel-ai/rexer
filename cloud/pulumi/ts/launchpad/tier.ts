import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import { nameof } from "../lib/util"
import * as kafkatopics from "../kafkatopics";
import * as mysql from "../mysql"
import * as httpserver from "../http-server";
import * as countaggr from "../countaggr";
import * as configs from "../configs";
import * as ingress from "../ingress";
import * as ns from "../k8s-ns";

import * as process from "process";

type inputType = {
    tierId: number
    // aws and k8s configuration.
    roleArn: string,
    region: string,
    kubeconfig: string,
    namespace: string,
    // kafka configuration.
    topicNames: string[],
    bootstrapServer: string,
    kafkaApiKey: string,
    kafkaApiSecret: pulumi.Output<string>,
    // db configuration.
    db: string,
    dbUsername: string,
    dbPassword: pulumi.Output<string>,
    dbEndpoint: string,
    // redis configuration.
    redisEndpoint: string,
    // elasticache configuration.
    cachePrimaryEndpoint: string,
    cacheReplicaEndpoint: string,
    // ingress configuration.
    subnetIds: string[],
    loadBalancerScheme: string,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        tierId: Number(config.require(nameof<inputType>("tierId"))),

        bootstrapServer: config.require(nameof<inputType>("bootstrapServer")),
        topicNames: config.requireObject(nameof<inputType>("topicNames")),
        kafkaApiKey: config.require(nameof<inputType>("kafkaApiKey")),
        kafkaApiSecret: config.requireSecret(nameof<inputType>("kafkaApiSecret")),

        db: config.require(nameof<inputType>("db")),
        dbUsername: config.require(nameof<inputType>("dbUsername")),
        dbPassword: config.requireSecret(nameof<inputType>("dbPassword")),
        dbEndpoint: config.require(nameof<inputType>("dbEndpoint")),

        roleArn: config.require(nameof<inputType>("roleArn")),
        region: config.require(nameof<inputType>("region")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        namespace: config.require(nameof<inputType>("namespace")),

        redisEndpoint: config.require(nameof<inputType>("redisEndpoint")),
        cachePrimaryEndpoint: config.require(nameof<inputType>("cachePrimaryEndpoint")),
        cacheReplicaEndpoint: config.require(nameof<inputType>("cacheReplicaEndpoint")),

        subnetIds: config.requireObject(nameof<inputType>("subnetIds")),
        loadBalancerScheme: config.require(nameof<inputType>("loadBalancerScheme")),
    };
};

const setupPlugins = async (stack: pulumi.automation.Stack) => {
    // TODO: aggregate plugins from all projects. If there are multiple versions
    // of the same plugin in different projects, we might want to use the latest.
    let plugins: { [key: string]: string } = {
        ...kafkatopics.plugins,
        ...mysql.plugins,
        ...configs.plugins,
        ...httpserver.plugins,
        ...countaggr.plugins,
        ...ingress.plugins,
        ...ns.plugins,
    }
    console.info("installing plugins...");
    for (var key in plugins) {
        await stack.workspace.installPlugin(key, plugins[key])
    }
    console.info("plugins installed");
}

// This is our pulumi program in "inline function" form
const setupResources = async () => {
    const input = parseConfig();
    // setup kakfa topics.
    const kafkaTopic = await kafkatopics.setup({
        apiKey: input.kafkaApiKey,
        apiSecret: input.kafkaApiSecret,
        topicNames: input.topicNames,
        bootstrapServer: input.bootstrapServer,
    })
    // setup mysql db.
    // Uncomment this once we have the ability to create a db in a remote VPC.
    // const sqlDB = await mysql.setup({
    //     username: input.dbUsername,
    //     password: input.dbPassword,
    //     endpoint: input.dbEndpoint,
    //     db: input.db,
    // })
    // setup k8s namespace.
    const namespace = await ns.setup({
        namespace: input.namespace,
        kubeconfig: input.kubeconfig,
    });
    // setup configs after resources are setup.
    const configsOutput = pulumi.all([input.dbPassword, input.kafkaApiSecret]).apply(async ([dbPassword, kafkaPassword]) => {
        return await configs.setup({
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
            tierConfig: { "tier_id": String(input.tierId) } as configs.map,
            redisConfig: pulumi.output({
                "addr": input.redisEndpoint,
            } as configs.map),
            cacheConfig: pulumi.output({
                "primary": input.cachePrimaryEndpoint,
                "replica": input.cacheReplicaEndpoint,
            } as configs.map),
            dbConfig: pulumi.output({
                "host": input.dbEndpoint,
                "db": input.db,
                "username": input.dbUsername,
                "password": dbPassword,
            } as configs.map),
            kafkaConfig: pulumi.output({
                "server": input.bootstrapServer,
                "username": input.kafkaApiKey,
                "password": kafkaPassword,
            } as configs.map),
        })
    })
    // setup ingress.
    await ingress.setup({
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
        subnetIds: input.subnetIds,
        loadBalancerScheme: input.loadBalancerScheme,
    })
    // setup countaggr after configs are setup.
    configsOutput.apply(async () => {
        return await countaggr.setup({
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
        });
    })
    // setup httpserver after ingress and configs are setup.
    configsOutput.apply(async () => {
        return await httpserver.setup({
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
        });
    })
};

type TierConf = {
    tierId: number
    // kafka configuration.
    topicNames: string[],
    bootstrapServer: string,
    kafkaApiKey: string,
    kafkaApiSecret: string,
    // db configuration.
    db: string,
    dbUsername: string,
    dbPassword: string,
    dbEndpoint: string,
    // aws and k8s configuration.
    roleArn: string,
    region: string,
    kubeconfig: string,
    namespace: string,
    // redis configuration.
    redisEndpoint: string,
    // elasticache configuration.
    cachePrimaryEndpoint: string,
    cacheReplicaEndpoint: string,
    // ingress configuration.
    subnetIds: string[],
    loadBalancerScheme: string,
}

const setupTier = async (args: TierConf, destroy?: boolean) => {
    const projectName = `launchpad`
    const stackName = `fennel/${projectName}/tier-${args.tierId}`

    console.info("initializing stack");
    // Create our stack
    const stackArgs: InlineProgramArgs = {
        projectName,
        stackName,
        program: setupResources,
    };
    // create (or select if one already exists) a stack that uses our inline program
    const stack = await LocalWorkspace.createOrSelectStack(stackArgs);
    console.info("successfully initialized stack");

    await setupPlugins(stack)

    console.info("setting up config");

    await stack.setConfig(nameof<inputType>("tierId"), { value: String(args.tierId) })

    await stack.setConfig(nameof<inputType>("bootstrapServer"), { value: args.bootstrapServer })
    await stack.setConfig(nameof<inputType>("kafkaApiKey"), { value: args.kafkaApiKey })
    await stack.setConfig(nameof<inputType>("kafkaApiSecret"), { value: args.kafkaApiSecret, secret: true })
    await stack.setConfig(nameof<inputType>("topicNames"), { value: JSON.stringify(args.topicNames) })

    await stack.setConfig(nameof<inputType>("db"), { value: args.db })
    await stack.setConfig(nameof<inputType>("dbUsername"), { value: args.dbUsername })
    await stack.setConfig(nameof<inputType>("dbPassword"), { value: args.dbPassword, secret: true })
    await stack.setConfig(nameof<inputType>("dbEndpoint"), { value: args.dbEndpoint })

    await stack.setConfig(nameof<inputType>("roleArn"), { value: args.roleArn })
    await stack.setConfig(nameof<inputType>("region"), { value: args.region })
    await stack.setConfig(nameof<inputType>("kubeconfig"), { value: args.kubeconfig })
    await stack.setConfig(nameof<inputType>("namespace"), { value: args.namespace })

    await stack.setConfig(nameof<inputType>("redisEndpoint"), { value: args.redisEndpoint })
    await stack.setConfig(nameof<inputType>("cachePrimaryEndpoint"), { value: args.cachePrimaryEndpoint })
    await stack.setConfig(nameof<inputType>("cacheReplicaEndpoint"), { value: args.cacheReplicaEndpoint })

    await stack.setConfig(nameof<inputType>("subnetIds"), { value: JSON.stringify(args.subnetIds) })
    await stack.setConfig(nameof<inputType>("loadBalancerScheme"), { value: args.loadBalancerScheme })

    console.info("config set");

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
