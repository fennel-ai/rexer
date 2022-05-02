import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import { nameof } from "../lib/util"
import * as kafkatopics from "../kafkatopics";
import * as kafkaconnectors from "../kafkaconnectors";
import * as mysql from "../mysql"
import * as httpserver from "../http-server";
import * as countaggr from "../countaggr";
import * as apiserver from "../apiserver";
import * as configs from "../configs";
import * as ingress from "../ingress";
import * as ns from "../k8s-ns";
import * as glue from "../glue";
import * as modelStore from "../model-store";
import * as sagemaker from "../sagemaker";
import * as offlineAggregateStorage from "../offline-aggregate-storage";
import * as offlineAggregateOutput from "../offline-aggregate-output";
import * as offlineAggregateKafkaConnector from "../offline-aggregate-kafka-connector";
import * as offlineAggregateGlueJob from "../offline-aggregate-glue-job";

import * as process from "process";

export type HttpServerConf = {
    replicas: number,
    enforceReplicaIsolation: boolean,
}

export type ApiServerConf = {
    replicas: number,
    enforceReplicaIsolation: boolean,
    storageclass: string,
}

export type CountAggrConf = {
    enforceServiceIsolation: boolean,
}

export type IngressConf = {
    usePublicSubnets: boolean,
    loadBalancerScheme: string,
}

export type TierConf = {
    planeId: number,
    httpServerConf?: HttpServerConf,
    countAggrConf?: CountAggrConf,
    apiServerConf?: ApiServerConf,
    ingressConf?: IngressConf,
}

type inputType = {
    tierId: number,
    planeId: number,
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
    // kafka connectors configuration
    confUsername: string,
    confPassword: string,
    clusterId: string,
    environmentId: string,
    connUserAccessKey: string,
    connUserSecret: string,
    connBucketName: string,
    // db configuration.
    db: string,
    dbUsername: string,
    dbPassword: pulumi.Output<string>,
    dbEndpoint: string,
    // redis configuration.
    redisEndpoint: string,
    // elasticache configuration.
    cachePrimaryEndpoint: string,
    // ingress configuration.
    subnetIds: string[],
    loadBalancerScheme: string,
    // glue configuration
    glueSourceBucket: string,
    glueSourceScript: string,
    glueTrainingDataBucket: string,
    // offline aggregate glue job configuration
    offlineAggregateSourceBucket: string,
    offlineAggregateSourceFiles: Record<string, string>,
    httpServerConf?: HttpServerConf,
    countAggrConf?: CountAggrConf,
    apiServerConf?: ApiServerConf,
    nodeInstanceRole: string,
    vpcId: string,
    connectedSecurityGroups: Record<string, string>,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        tierId: config.requireNumber(nameof<inputType>("tierId")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),

        bootstrapServer: config.require(nameof<inputType>("bootstrapServer")),
        topicNames: config.requireObject(nameof<inputType>("topicNames")),
        kafkaApiKey: config.require(nameof<inputType>("kafkaApiKey")),
        kafkaApiSecret: config.requireSecret(nameof<inputType>("kafkaApiSecret")),

        confUsername: config.require(nameof<inputType>("confUsername")),
        confPassword: config.require(nameof<inputType>("confPassword")),
        clusterId: config.require(nameof<inputType>("clusterId")),
        environmentId: config.require(nameof<inputType>("environmentId")),
        connUserAccessKey: config.require(nameof<inputType>("connUserAccessKey")),
        connUserSecret: config.require(nameof<inputType>("connUserSecret")),
        connBucketName: config.require(nameof<inputType>("connBucketName")),

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

        subnetIds: config.requireObject(nameof<inputType>("subnetIds")),
        loadBalancerScheme: config.require(nameof<inputType>("loadBalancerScheme")),

        glueSourceBucket: config.require(nameof<inputType>("glueSourceBucket")),
        glueSourceScript: config.require(nameof<inputType>("glueSourceScript")),
        glueTrainingDataBucket: config.require(nameof<inputType>("glueTrainingDataBucket")),

        offlineAggregateSourceBucket: config.require(nameof<inputType>("offlineAggregateSourceBucket")),
        offlineAggregateSourceFiles: config.requireObject(nameof<inputType>("offlineAggregateSourceFiles")),

        httpServerConf: config.getObject(nameof<inputType>("httpServerConf")),
        countAggrConf: config.getObject(nameof<inputType>("countAggrConf")),
        apiServerConf: config.getObject(nameof<inputType>("apiServerConf")),

        nodeInstanceRole: config.require(nameof<inputType>("nodeInstanceRole")),

        vpcId: config.require(nameof<inputType>("vpcId")),
        connectedSecurityGroups: config.requireObject(nameof<inputType>("connectedSecurityGroups")),
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
        ...apiserver.plugins,
        ...ingress.plugins,
        ...ns.plugins,
        ...kafkaconnectors.plugins,
        ...glue.plugins,
        ...modelStore.plugins,
        ...sagemaker.plugins,
        ...offlineAggregateStorage.plugins,
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
    const offlineAggregateStorageBucket = await offlineAggregateStorage.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
    })
    // setup kafka connector to s3 bucket for the action and feature log topics.
    const kafkaConnectors = await kafkaconnectors.setup({
        tierId: input.tierId,
        username: input.confUsername,
        password: input.confPassword,
        clusterId: input.clusterId,
        environmentId: input.environmentId,
        kafkaApiKey: input.kafkaApiKey,
        kafkaApiSecret: input.kafkaApiSecret,
        awsAccessKeyId: input.connUserAccessKey,
        awsSecretAccessKey: input.connUserSecret,
        s3BucketName: input.connBucketName,
    })
    // setup kafka connectors to s3 bucket for offline aggregate data
    const offlineAggregateConnector = await offlineAggregateKafkaConnector.setup({
        tierId: input.tierId,
        username: input.confUsername,
        password: input.confPassword,
        clusterId: input.clusterId,
        environmentId: input.environmentId,
        kafkaApiKey: input.kafkaApiKey,
        kafkaApiSecret: input.kafkaApiSecret,
        awsAccessKeyId: offlineAggregateStorageBucket.userAccessKeyId,
        awsSecretAccessKey: offlineAggregateStorageBucket.userSecretAccessKey,
        s3BucketName: offlineAggregateStorageBucket.bucketName,
    })
    // setup offline aggregate output bucket
    const offlineAggregateOutputBucket = await offlineAggregateOutput.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
    })

    // setup offline aggregate glue job
    const offlineAggregateGlueJobOutput = await offlineAggregateGlueJob.setup({
        tierId: input.tierId,
        region: input.region,
        roleArn: input.roleArn,
        sourceBucket: input.offlineAggregateSourceBucket,
        storageBucket: offlineAggregateStorageBucket.bucketName,
        outputBucket: offlineAggregateOutputBucket.bucketName,
        sourceFiles: input.offlineAggregateSourceFiles,
        nodeInstanceRole: input.nodeInstanceRole,
    })

    // setup mysql db.
    // Comment this when direct connection to the db instance is not possible.
    // This will usually be when trying to setup a tier in a customer vpc, which
    // should usually be done through the bridge.
    const sqlDB = await mysql.setup({
        username: input.dbUsername,
        password: input.dbPassword,
        endpoint: input.dbEndpoint,
        db: `t_${input.tierId}_db`,
    })
    // setup k8s namespace.
    const namespace = await ns.setup({
        namespace: input.namespace,
        kubeconfig: input.kubeconfig,
    });
    // setup model store for this tier
    const modelStoreOutput = await modelStore.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        nodeInstanceRole: input.nodeInstanceRole,
    })
    // setup sagemaker endpoint related resources
    const sagemakerOutput = await sagemaker.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        planeId: input.planeId,
        vpcId: input.vpcId,
        nodeInstanceRole: input.nodeInstanceRole,
        connectedSecurityGroups: input.connectedSecurityGroups,
        modelStoreBucket: modelStoreOutput.modelStoreBucket,
    })
    // setup configs after resources are setup.
    const configsOutput = pulumi.all(
        [input.dbPassword, input.kafkaApiSecret, sagemakerOutput.roleArn, sagemakerOutput.subnetIds,
        sagemakerOutput.securityGroup, offlineAggregateGlueJobOutput.jobNames]).apply(async ([dbPassword, kafkaPassword, sagemakerRole, subnetIds, sagemakerSg, jobNames]) => {
            // transform jobname map to string with the format `key1=val1 key2=val2`
            let jobNamesStr = "";
            Object.entries(jobNames).forEach(([agg, jobName]) => jobNamesStr += `${agg}=${jobName},`);
            // remove the last `,`
            jobNamesStr = jobNamesStr.substring(0, jobNamesStr.length - 1);
            console.log(jobNamesStr);
            return await configs.setup({
                kubeconfig: input.kubeconfig,
                namespace: input.namespace,
                tierConfig: { "tier_id": String(input.tierId) },
                redisConfig: pulumi.output({
                    "addr": input.redisEndpoint,
                } as Record<string, string>),
                cacheConfig: pulumi.output({
                    "primary": input.cachePrimaryEndpoint,
                } as Record<string, string>),
                dbConfig: pulumi.output({
                    "host": input.dbEndpoint,
                    "db": input.db,
                    "username": input.dbUsername,
                    "password": dbPassword,
                } as Record<string, string>),
                kafkaConfig: pulumi.output({
                    "server": input.bootstrapServer,
                    "username": input.kafkaApiKey,
                    "password": kafkaPassword,
                } as Record<string, string>),
                modelServingConfig: pulumi.output({
                    "region": input.region,
                    "executionRole": sagemakerRole,
                    "privateSubnets": subnetIds.join(","),
                    "securityGroup": sagemakerSg,
                    "modelStoreBucket": modelStoreOutput.modelStoreBucket,
                    // pass tierId as the endpoint name
                    "modelStoreEndpoint": `t-${input.tierId}`,
                } as Record<string, string>),
                glueConfig: pulumi.output({
                    "region": input.region,
                    "jobNameByAgg": jobNamesStr,
                } as Record<string, string>),
            })
        })
    // setup ingress.
    const ingressOutput = await ingress.setup({
        roleArn: input.roleArn,
        region: input.region,
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
        subnetIds: input.subnetIds,
        loadBalancerScheme: input.loadBalancerScheme,
        tierId: input.tierId,
    })
    // setup glue
    const glueOutput = await glue.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        planeId: input.planeId,
        sourceBucket: input.glueSourceBucket,
        trainingDataBucket: input.glueTrainingDataBucket,
        script: input.glueSourceScript,
    })
    configsOutput.apply(async () => {
        // setup http-server and countaggr after configs are setup.
        const httpServerOutput = await httpserver.setup({
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
            tierId: input.tierId,
            replicas: input.httpServerConf?.replicas,
            enforceReplicaIsolation: input.httpServerConf?.enforceReplicaIsolation,
        });
        // This there is an affinity requirement on http-server and countaggr pods, schedule the http-server pod first
        // and let countaggr depend on it's output so that affinity requirements do not unexpected behavior
        await countaggr.setup({
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
            tierId: input.tierId,
            enforceServiceIsolation: input.countAggrConf?.enforceServiceIsolation,
            httpServerAppLabels: httpServerOutput.appLabels,
        });
        // set api-server only if the API Server configuration is defined
        if (input.apiServerConf !== undefined) {
            await apiserver.setup({
                roleArn: input.roleArn,
                region: input.region,
                kubeconfig: input.kubeconfig,
                namespace: input.namespace,
                tierId: input.tierId,
                replicas: input.apiServerConf?.replicas,
                enforceReplicaIsolation: input.apiServerConf?.enforceReplicaIsolation,
                storageclass: input.apiServerConf?.storageclass,
            })
        }
    })
    return {
        "ingress": ingressOutput,
        "modelStore": modelStoreOutput,
    }
};

type TierInput = {
    tierId: number,
    planeId: number,
    // kafka configuration.
    topicNames: string[],
    bootstrapServer: string,
    kafkaApiKey: string,
    kafkaApiSecret: string,

    // connector configuration
    confUsername: string,
    confPassword: string,
    clusterId: string,
    environmentId: string,
    connUserAccessKey: string,
    connUserSecret: string,
    connBucketName: string,

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
    // ingress configuration.
    subnetIds: string[],
    loadBalancerScheme: string,
    // glue configuration.
    glueSourceBucket: string,
    glueSourceScript: string,
    glueTrainingDataBucket: string,

    // offline aggregate glue job configuration
    offlineAggregateSourceBucket: string,
    offlineAggregateSourceFiles: Record<string, string>,

    // http server configuration
    httpServerConf?: HttpServerConf,

    // countaggr configuration
    countAggrConf?: CountAggrConf,

    // api-server configuration
    apiServerConf?: ApiServerConf,

    // model store configuration
    nodeInstanceRole: string,

    // sagemaker configuration
    vpcId: string,
    connectedSecurityGroups: Record<string, string>,
}

const setupTier = async (args: TierInput, destroy?: boolean) => {
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
    await stack.setConfig(nameof<inputType>("planeId"), { value: String(args.planeId) })

    await stack.setConfig(nameof<inputType>("bootstrapServer"), { value: args.bootstrapServer })
    await stack.setConfig(nameof<inputType>("kafkaApiKey"), { value: args.kafkaApiKey })
    await stack.setConfig(nameof<inputType>("kafkaApiSecret"), { value: args.kafkaApiSecret, secret: true })
    await stack.setConfig(nameof<inputType>("topicNames"), { value: JSON.stringify(args.topicNames) })

    await stack.setConfig(nameof<inputType>("bootstrapServer"), { value: args.bootstrapServer })
    await stack.setConfig(nameof<inputType>("kafkaApiKey"), { value: args.kafkaApiKey })
    await stack.setConfig(nameof<inputType>("kafkaApiSecret"), { value: args.kafkaApiSecret, secret: true })

    await stack.setConfig(nameof<inputType>("confUsername"), { value: args.confUsername })
    await stack.setConfig(nameof<inputType>("confPassword"), { value: args.confPassword })
    await stack.setConfig(nameof<inputType>("clusterId"), { value: args.clusterId })
    await stack.setConfig(nameof<inputType>("environmentId"), { value: args.environmentId })
    await stack.setConfig(nameof<inputType>("connUserAccessKey"), { value: args.connUserAccessKey })
    await stack.setConfig(nameof<inputType>("connUserSecret"), { value: args.connUserSecret })
    await stack.setConfig(nameof<inputType>("connBucketName"), { value: args.connBucketName })

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

    await stack.setConfig(nameof<inputType>("subnetIds"), { value: JSON.stringify(args.subnetIds) })
    await stack.setConfig(nameof<inputType>("loadBalancerScheme"), { value: args.loadBalancerScheme })

    await stack.setConfig(nameof<inputType>("glueSourceBucket"), { value: args.glueSourceBucket })
    await stack.setConfig(nameof<inputType>("glueSourceScript"), { value: args.glueSourceScript })
    await stack.setConfig(nameof<inputType>("glueTrainingDataBucket"), { value: args.glueTrainingDataBucket })

    await stack.setConfig(nameof<inputType>("offlineAggregateSourceBucket"), { value: args.offlineAggregateSourceBucket })
    await stack.setConfig(nameof<inputType>("offlineAggregateSourceFiles"), { value: JSON.stringify(args.offlineAggregateSourceFiles) })

    if (args.httpServerConf !== undefined) {
        await stack.setConfig(nameof<inputType>("httpServerConf"), { value: JSON.stringify(args.httpServerConf) })
    }

    if (args.countAggrConf !== undefined) {
        await stack.setConfig(nameof<inputType>("countAggrConf"), { value: JSON.stringify(args.countAggrConf) })
    }

    if (args.apiServerConf !== undefined) {
        await stack.setConfig(nameof<inputType>("apiServerConf"), { value: JSON.stringify(args.apiServerConf) })
    }

    await stack.setConfig(nameof<inputType>("nodeInstanceRole"), { value: args.nodeInstanceRole })

    await stack.setConfig(nameof<inputType>("vpcId"), { value: args.vpcId })
    await stack.setConfig(nameof<inputType>("connectedSecurityGroups"), { value: JSON.stringify(args.connectedSecurityGroups) })

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
