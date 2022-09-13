import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import { nameof } from "../lib/util"
import * as airbyte from "../airbyte";
import * as kafkatopics from "../kafkatopics";
import * as kafkaconnectors from "../kafkaconnectors";
import * as mysql from "../mysql"
import * as httpserver from "../http-server";
import * as queryserver from "../query-server";
import * as queryserverShadow from "../query-server-shadow";
import * as nitrous from "../nitrous";
import * as countaggr from "../countaggr";
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
import * as pprofBucket from "../pprof-bucket";
import * as tierEksPermissions from "../tier-eks-permissions";
import * as countersCleanup from "../counters-cleanup";
import * as unleash from "../unleash";
import * as mirrorMaker from "../mirror-maker";
import * as util from "../lib/util";

import * as process from "process";

const DEFAULT_SAGEMAKER_INSTANCE_TYPE = "ml.c5.large";
const DEFAULT_SAGEMAKER_INSTANCE_COUNT = 1;

export type HttpServerConf = {
    podConf?: util.PodConf
}

export type QueryServerConf = {
    podConf?: util.PodConf
}

export type CountAggrConf = {
    // replicas are currently not set, but in the future they might be configured
    podConf?: util.PodConf
}

export type CounterCleanupConf = {
    // replicas are currently not set, but in the future they might be configured
    podConf?: util.PodConf
}


export type SagemakerConf = {
    instanceType: string,
    instanceCount: number,
}

export type AirbyteConf = {
    // whether airbyte server to be made externally available
    //
    // NOTE: This should be enabled only for test/staging tiers
    publicServer?: boolean,
}

export type TierMskConf = {
    mskUsername: string,
    mskPassword: string,
    // comma separated bootstrap servers in and across multiple AZs
    bootstrapBrokers: string,
}

export type TierConf = {
    // Should be set to false, when deleting the tier
    //
    // Else, individual data storage resources, if they are to be deleted, should be set to false and the stack should
    // be updated
    //
    // NOTE: Please add a justification if this value is being set to False and the configuration is being checked-in
    protectResources: boolean,
    planeId: number,
    enableNitrous?: boolean,
    createTopicsInMsk?: boolean,
    mirrorMakerConf?: mirrorMaker.MirrorMakerConf,
    httpServerConf?: HttpServerConf,
    queryServerConf?: QueryServerConf,
    countAggrConf?: CountAggrConf,
    counterCleanupConf?: CounterCleanupConf,
    ingressConf?: util.IngressConf,
    sagemakerConf?: SagemakerConf,
    airbyteConf?: AirbyteConf,
    plan?: util.Plan,
    requestLimit?: number,
    customerId?: number,
}

type inputType = {
    protect: boolean,
    tierId: number,
    planeId: number,
    // aws and k8s configuration.
    roleArn: string,
    region: string,
    kubeconfig: string,
    namespace: string,
    nodeInstanceRole: string,
    vpcId: string,
    connectedSecurityGroups: Record<string, string>,
    // kafka configuration.
    topics: kafkatopics.topicConf[],
    bootstrapServer: string,
    kafkaApiKey: string,
    kafkaApiSecret: pulumi.Output<string>,

    // create topics in msk for the tier
    createTopicsInMsk?: boolean,
    mirrorMakerConf?: mirrorMaker.MirrorMakerConf,

    // msk configuration
    mskConf?: TierMskConf,

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
    // unleash db configuration.
    postgresDbEndpoint: string,
    postgresDbPort: number,
    // redis configuration.
    redisEndpoint: string,
    // elasticache configuration.
    cachePrimaryEndpoint: string,
    // ingress configuration.
    subnetIds: string[],
    loadBalancerScheme: string,
    ingressUseDedicatedMachines?: boolean,
    ingressReplicas?: number,
    clusterName: string,
    nodeInstanceRoleArn: string,

    // glue configuration
    glueSourceBucket: string,
    glueSourceScript: string,
    glueTrainingDataBucket: string,
    // offline aggregate glue job configuration
    offlineAggregateSourceBucket: string,
    offlineAggregateSourceFiles: Record<string, string>,
    // otel collector endpoints
    otelCollectorEndpoint: string,
    otelCollectorHttpEndpoint: string,
    // service configurations.
    httpServerConf?: HttpServerConf,
    queryServerConf?: QueryServerConf,
    countAggrConf?: CountAggrConf,
    counterCleanupConf?: CounterCleanupConf,
    enableNitrous?: boolean,

    // third-party services configuration
    sagemakerConf?: SagemakerConf,
    milvusEndpoint: string,
    airbyteConf?: AirbyteConf,
    plan?: util.Plan,
    requestLimit?: number,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        protect: config.requireBoolean(nameof<inputType>("protect")),
        tierId: config.requireNumber(nameof<inputType>("tierId")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),

        bootstrapServer: config.require(nameof<inputType>("bootstrapServer")),
        topics: config.requireObject(nameof<inputType>("topics")),
        kafkaApiKey: config.require(nameof<inputType>("kafkaApiKey")),
        kafkaApiSecret: config.requireSecret(nameof<inputType>("kafkaApiSecret")),

        mskConf: config.getObject(nameof<inputType>("mskConf")),

        createTopicsInMsk: config.getBoolean(nameof<inputType>("createTopicsInMsk")),
        mirrorMakerConf: config.getObject(nameof<inputType>("mirrorMakerConf")),

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

        postgresDbEndpoint: config.require(nameof<inputType>("postgresDbEndpoint")),
        postgresDbPort: config.requireNumber(nameof<inputType>("postgresDbPort")),

        roleArn: config.require(nameof<inputType>("roleArn")),
        region: config.require(nameof<inputType>("region")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        namespace: config.require(nameof<inputType>("namespace")),

        redisEndpoint: config.require(nameof<inputType>("redisEndpoint")),
        cachePrimaryEndpoint: config.require(nameof<inputType>("cachePrimaryEndpoint")),

        subnetIds: config.requireObject(nameof<inputType>("subnetIds")),
        loadBalancerScheme: config.require(nameof<inputType>("loadBalancerScheme")),
        ingressUseDedicatedMachines: config.getBoolean(nameof<inputType>("ingressUseDedicatedMachines")),
        ingressReplicas: config.getNumber(nameof<inputType>("ingressReplicas")),
        clusterName: config.require(nameof<inputType>("clusterName")),
        nodeInstanceRoleArn: config.require(nameof<inputType>("nodeInstanceRoleArn")),

        glueSourceBucket: config.require(nameof<inputType>("glueSourceBucket")),
        glueSourceScript: config.require(nameof<inputType>("glueSourceScript")),
        glueTrainingDataBucket: config.require(nameof<inputType>("glueTrainingDataBucket")),

        offlineAggregateSourceBucket: config.require(nameof<inputType>("offlineAggregateSourceBucket")),
        offlineAggregateSourceFiles: config.requireObject(nameof<inputType>("offlineAggregateSourceFiles")),

        otelCollectorEndpoint: config.require(nameof<inputType>("otelCollectorEndpoint")),
        otelCollectorHttpEndpoint: config.require(nameof<inputType>("otelCollectorHttpEndpoint")),

        httpServerConf: config.getObject(nameof<inputType>("httpServerConf")),
        queryServerConf: config.getObject(nameof<inputType>("queryServerConf")),
        countAggrConf: config.getObject(nameof<inputType>("countAggrConf")),
        counterCleanupConf: config.getObject(nameof<inputType>("counterCleanupConf")),
        enableNitrous: config.getObject(nameof<inputType>("enableNitrous")),

        sagemakerConf: config.getObject(nameof<inputType>("sagemakerConf")),

        nodeInstanceRole: config.require(nameof<inputType>("nodeInstanceRole")),

        vpcId: config.require(nameof<inputType>("vpcId")),
        connectedSecurityGroups: config.requireObject(nameof<inputType>("connectedSecurityGroups")),

        milvusEndpoint: config.require(nameof<inputType>("milvusEndpoint")),

        airbyteConf: config.getObject(nameof<inputType>("airbyteConf")),

        plan: config.getNumber(nameof<inputType>("plan")),
        requestLimit: config.getNumber(nameof<inputType>("requestLimit"))
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
        ...kafkaconnectors.plugins,
        ...glue.plugins,
        ...modelStore.plugins,
        ...sagemaker.plugins,
        ...offlineAggregateStorage.plugins,
        ...pprofBucket.plugins,
        ...countersCleanup.plugins,
        ...queryserver.plugins,
        ...unleash.plugins,
        ...airbyte.plugins,
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
        topics: input.topics,
        bootstrapServer: input.bootstrapServer,
        protect: input.protect,

        createInMsk: input.createTopicsInMsk,
        mskApiKey: input.mskConf?.mskUsername,
        mskApiSecret: input.mskConf?.mskPassword,
        mskBootstrapServers: input.mskConf?.bootstrapBrokers,
    });
    if (input.mirrorMakerConf) {
        const mirrorMakerOutput = await mirrorMaker.setup({
            tierId: input.tierId,
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,

            topics: input.topics,
            conf: input.mirrorMakerConf!,

            mskPassword: input.mskConf!.mskPassword,
            mskUsername: input.mskConf!.mskUsername,
            mskBootstrapServers: input.mskConf!.bootstrapBrokers,

            confluentPassword: input.kafkaApiSecret,
            confluentUsername: input.kafkaApiKey,
            confluentBootstrapServers: input.bootstrapServer,
        });
    }
    const offlineAggregateStorageBucket = await offlineAggregateStorage.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        protect: input.protect,
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
        protect: input.protect,
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
        protect: input.protect,
    })
    // setup offline aggregate output bucket
    const offlineAggregateOutputBucket = await offlineAggregateOutput.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        protect: input.protect,
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
        protect: input.protect,
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
        protect: input.protect,
    })
    // setup sagemaker endpoint related resources
    const sagemakerOutput = await sagemaker.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        planeId: input.planeId,
        vpcId: input.vpcId,
        connectedSecurityGroups: input.connectedSecurityGroups,
        modelStoreBucket: modelStoreOutput.modelStoreBucket,
    });

    // setup unleash
    const unleashOutput = await unleash.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        namespace: input.namespace,
        unleashDbEndpoint: input.postgresDbEndpoint,
        unleashDbPort: input.postgresDbPort,
        kubeconfig: input.kubeconfig,
        protect: input.protect,
    });

    // create a s3 bucket for pprof profiles
    const pprofBucketOutput = await pprofBucket.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        protect: input.protect,
    });

    // airbyte configuration
    //
    // this is empty for tiers which do not configure airbyte
    let airbyteEndpoint: pulumi.Output<string> = pulumi.output("");
    if (input.airbyteConf !== undefined) {
        const airbyteOutput = await airbyte.setup({
            region: input.region,
            roleArn: input.roleArn,
            tierId: input.tierId,
            namespace: input.namespace,
            dbEndpoint: input.postgresDbEndpoint,
            dbPort: input.postgresDbPort,
            kubeconfig: input.kubeconfig,
            protect: input.protect,
            publicServer: input.airbyteConf.publicServer,
        });
        airbyteEndpoint = airbyteOutput.endpoint;
    }

    // setup configs after resources are setup.
    const configsOutput = pulumi.all(
        [input.dbPassword, input.kafkaApiSecret, sagemakerOutput.roleArn, sagemakerOutput.subnetIds,
        sagemakerOutput.securityGroup, offlineAggregateGlueJobOutput.jobNames, offlineAggregateOutputBucket.bucketName, airbyteEndpoint]).apply(async ([dbPassword, kafkaPassword, sagemakerRole, subnetIds, sagemakerSg, jobNames, offlineAggrOutputBucket, airbyteServerEndpoint]) => {
            // transform jobname map to string with the format `key1=val1 key2=val2`
            let jobNamesStr = "";
            Object.entries(jobNames).forEach(([agg, jobName]) => jobNamesStr += `${agg}=${jobName},`);
            // remove the last `,`
            jobNamesStr = jobNamesStr.substring(0, jobNamesStr.length - 1);
            console.log(jobNamesStr);
            return await configs.setup({
                kubeconfig: input.kubeconfig,
                namespace: input.namespace,
                tierConfig: {
                    "tier_id": String(input.tierId),
                    "plane_id": String(input.planeId),
                },
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
                mskConfig: pulumi.output({
                    "mskServers": input.mskConf?.bootstrapBrokers || "",
                    "mskUsername": input.mskConf?.mskUsername || "",
                    "mskPassword": input.mskConf?.mskPassword || "",
                } as Record<string, string>),
                modelServingConfig: pulumi.output({
                    "region": input.region,
                    "executionRole": sagemakerRole,
                    "privateSubnets": subnetIds.join(","),
                    "securityGroup": sagemakerSg,
                    "modelStoreBucket": modelStoreOutput.modelStoreBucket,
                    // pass tierId as the endpoint name
                    "modelStoreEndpoint": `t-${input.tierId}`,
                    "instanceType": input.sagemakerConf?.instanceType || DEFAULT_SAGEMAKER_INSTANCE_TYPE,
                    "instanceCount": `${input.sagemakerConf?.instanceCount || DEFAULT_SAGEMAKER_INSTANCE_COUNT}`,
                } as Record<string, string>),
                glueConfig: pulumi.output({
                    "region": input.region,
                    "jobNameByAgg": jobNamesStr,
                } as Record<string, string>),
                unleashConfig: pulumi.output({
                    "endpoint": unleashOutput.unleashEndpoint,
                } as Record<string, string>),
                otelCollectorConfig: pulumi.output({
                    "endpoint": input.otelCollectorEndpoint,
                    "httpEndpoint": input.otelCollectorHttpEndpoint,
                } as Record<string, string>),
                offlineAggregateOutputConfig: pulumi.output({
                    "bucket": offlineAggrOutputBucket,
                } as Record<string, string>),
                milvusConfig: pulumi.output({
                    "endpoint": input.milvusEndpoint,
                } as Record<string, string>),
                pprofConfig: pulumi.output({
                    "bucket": pprofBucketOutput.pprofStoreBucket,
                } as Record<string, string>),
                nitrousConfig: pulumi.output({
                    "addr": input.enableNitrous ? `${nitrous.name}.${nitrous.namespace}:${nitrous.servicePort}` : "",
                } as Record<string, string>),
                airbyteConfig: pulumi.output({
                    "endpoint": airbyteServerEndpoint,
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
        scopeId: input.tierId,
        useDedicatedMachines: input.ingressUseDedicatedMachines,
        replicas: input.ingressReplicas,
        clusterName: input.clusterName,
        nodeRoleArn: input.nodeInstanceRoleArn,
        scope: util.Scope.TIER,
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

    let queryServerShadowBucketName: string | undefined;
    if (input.queryServerConf !== undefined) {
        queryServerShadowBucketName = `t-${input.tierId}-query-server-reqs`;
    }

    // setup tier level permissions on the EKS instance role before actually spinning up the jobs so that the
    // jobs don't get provisioned with permission errors
    await tierEksPermissions.setup({
        region: input.region,
        roleArn: input.roleArn,
        tierId: input.tierId,
        nodeInstanceRole: input.nodeInstanceRole,
        modelStoreBucket: modelStoreOutput.modelStoreBucket,
        pprofBucket: pprofBucketOutput.pprofStoreBucket,
        offlineAggregateOutputBucket: offlineAggregateOutputBucket.bucketName,
        queryServerShadowBucket: queryServerShadowBucketName,
    })

    configsOutput.apply(async () => {
        // setup services after configs are setup.

        // NOTE: We remove the concept of pod anti-affinity -> this was initially introduced to schedule
        // two pods of different services on different nodes always.
        //
        // This should, currently should be supported using managed node groups i.e. each pod it has to be independently
        // scheduled from a pod(s) of another service should it's dedicated node group and internally
        // replica isolation is still supported
        //
        // Going forward, we should ideally have each service/pod specify the resource expectations/limits
        // which kube scheduler uses to schedule them on nodes without us explicitly specifying node <-> pod
        // relationship

        // NOTE: HTTP and Query servers currently host the same binary. Query server only handles `/data/query` calls
        // whereas HTTP Server is still capable of hosting all APIs.
        //
        // We use Ambassador ingress to handle mappings. By-default Ambassador does a longest prefix match to choose
        // the backend/service to send the requests to. If Query server is configured, Ambassador will forward all
        // queries to it, else the calls are by-default sent to HTTP server since it allows all calls matching path
        // `/data/`
        await httpserver.setup({
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
            tierId: input.tierId,
            minReplicas: input.httpServerConf?.podConf?.minReplicas,
            maxReplicas: input.httpServerConf?.podConf?.maxReplicas,
            resourceConf: input.httpServerConf?.podConf?.resourceConf,
            useAmd64: input.httpServerConf?.podConf?.useAmd64,
            nodeLabels: input.httpServerConf?.podConf?.nodeLabels,
            pprofHeapAllocThresholdMegaBytes: input.httpServerConf?.podConf?.pprofHeapAllocThresholdMegaBytes,
        });

        // this sets up query server which is responsible for handling `/data/query` REST calls
        //
        // define this service only if the query server configuration is provided. If not, HTTP Server
        // creates a mapping
        if (input.queryServerConf !== undefined) {
            await queryserver.setup({
                roleArn: input.roleArn,
                region: input.region,
                kubeconfig: input.kubeconfig,
                namespace: input.namespace,
                tierId: input.tierId,
                minReplicas: input.queryServerConf?.podConf?.minReplicas,
                maxReplicas: input.queryServerConf?.podConf?.maxReplicas,
                resourceConf: input.queryServerConf?.podConf?.resourceConf,
                useAmd64: input.queryServerConf?.podConf?.useAmd64,
                nodeLabels: input.queryServerConf?.podConf?.nodeLabels,
                pprofHeapAllocThresholdMegaBytes: input.queryServerConf?.podConf?.pprofHeapAllocThresholdMegaBytes,
            });

            await queryserverShadow.setup({
                roleArn: input.roleArn,
                region: input.region,
                kubeconfig: input.kubeconfig,
                namespace: input.namespace,
                tierId: input.tierId,
                shadowBucketName: queryServerShadowBucketName!,
            });
        }

        // This there is an affinity requirement on http-server and countaggr pods, schedule the http-server pod first
        // and let countaggr depend on it's output so that affinity requirements do not unexpected behavior
        await countaggr.setup({
            roleArn: input.roleArn,
            region: input.region,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
            tierId: input.tierId,
            useAmd64: input.countAggrConf?.podConf?.useAmd64,
            nodeLabels: input.countAggrConf?.podConf?.nodeLabels,
        });
        await countersCleanup.setup({
            region: input.region,
            roleArn: input.roleArn,
            kubeconfig: input.kubeconfig,
            namespace: input.namespace,
            tierId: input.tierId,
            useAmd64: input.counterCleanupConf?.podConf?.useAmd64,
        });
    })
    return {
        "ingress": ingressOutput,
        "modelStore": modelStoreOutput,
        "planeId": input.planeId,
    }
};

type TierInput = {
    protect: boolean,

    tierId: number,
    planeId: number,
    // kafka configuration.
    topics: kafkatopics.topicConf[],
    bootstrapServer: string,
    kafkaApiKey: string,
    kafkaApiSecret: string,

    // create topics in msk
    createTopicsInMsk?: boolean,
    mirrorMakerConf?: mirrorMaker.MirrorMakerConf,

    // msk configuration
    mskConf?: TierMskConf,

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

    // unleash db configuration
    postgresDbEndpoint: string,
    postgresDbPort: number,

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
    ingressUseDedicatedMachines?: boolean,
    ingressReplicas?: number,
    clusterName: string,
    nodeInstanceRoleArn: string,

    // glue configuration.
    glueSourceBucket: string,
    glueSourceScript: string,
    glueTrainingDataBucket: string,

    // offline aggregate glue job configuration
    offlineAggregateSourceBucket: string,
    offlineAggregateSourceFiles: Record<string, string>,

    // otel collector configuration
    otelCollectorEndpoint: string,
    otelCollectorHttpEndpoint: string,

    // http server configuration
    httpServerConf?: HttpServerConf,

    // query server configuration
    queryServerConf?: QueryServerConf,

    // flag to enable nitrous.
    enableNitrous?: boolean,

    // countaggr configuration
    countAggrConf?: CountAggrConf,

    // counter cleanup configuration
    counterCleanupConf?: CounterCleanupConf,

    // model store configuration
    nodeInstanceRole: string,

    // sagemaker configuration
    vpcId: string,
    connectedSecurityGroups: Record<string, string>,
    sagemakerConf?: SagemakerConf,

    // milvus
    milvusEndpoint: string,

    // airbyte
    airbyteConf?: AirbyteConf,

    // Plan.
    plan?: util.Plan,

    // Request Limit on tier services.
    requestLimit?: number,

    // customer id.
    customerId?: number,
}

const setupTier = async (args: TierInput, preview?: boolean, destroy?: boolean) => {
    const projectName = `launchpad`
    const stackName = `fennel/${projectName}/tier-${args.tierId}`

    // validate that if nitrous is enabled, plane had created a MSK cluster
    if (args.enableNitrous !== undefined && args.enableNitrous && args.mskConf === undefined) {
        console.log('nitrous is enabled but plane did not configure a MSK cluster')
        process.exit(1);
    }

    // if create topics in MSK is enabled, msk conf must be present
    if (args.createTopicsInMsk !== undefined && args.createTopicsInMsk && args.mskConf === undefined) {
        console.log('mskConf must be configured to create tier topics in MSK')
        process.exit(1);
    }

    if (args.mirrorMakerConf !== undefined && args.mskConf === undefined) {
        console.log("mskConf must be configured to create mirror maker")
        process.exit(1);
    }

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

    await stack.setConfig(nameof<inputType>("protect"), { value: String(args.protect) })

    await stack.setConfig(nameof<inputType>("tierId"), { value: String(args.tierId) })
    await stack.setConfig(nameof<inputType>("planeId"), { value: String(args.planeId) })

    await stack.setConfig(nameof<inputType>("bootstrapServer"), { value: args.bootstrapServer })
    await stack.setConfig(nameof<inputType>("kafkaApiKey"), { value: args.kafkaApiKey })
    await stack.setConfig(nameof<inputType>("kafkaApiSecret"), { value: args.kafkaApiSecret, secret: true })
    await stack.setConfig(nameof<inputType>("topics"), { value: JSON.stringify(args.topics) })

    if (args.mskConf !== undefined) {
        await stack.setConfig(nameof<inputType>("mskConf"), { value: JSON.stringify(args.mskConf) });
    }

    if (args.createTopicsInMsk !== undefined) {
        await stack.setConfig(nameof<inputType>("createTopicsInMsk"), { value: JSON.stringify(args.createTopicsInMsk) })
    }

    if (args.mirrorMakerConf !== undefined) {
        await stack.setConfig(nameof<inputType>("mirrorMakerConf"), { value: JSON.stringify(args.mirrorMakerConf) });
    }

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

    await stack.setConfig(nameof<inputType>("postgresDbEndpoint"), { value: args.postgresDbEndpoint })
    await stack.setConfig(nameof<inputType>("postgresDbPort"), { value: String(args.postgresDbPort) })

    await stack.setConfig(nameof<inputType>("roleArn"), { value: args.roleArn })
    await stack.setConfig(nameof<inputType>("region"), { value: args.region })
    await stack.setConfig(nameof<inputType>("kubeconfig"), { value: args.kubeconfig })
    await stack.setConfig(nameof<inputType>("namespace"), { value: args.namespace })

    await stack.setConfig(nameof<inputType>("redisEndpoint"), { value: args.redisEndpoint })
    await stack.setConfig(nameof<inputType>("cachePrimaryEndpoint"), { value: args.cachePrimaryEndpoint })

    await stack.setConfig(nameof<inputType>("subnetIds"), { value: JSON.stringify(args.subnetIds) })
    await stack.setConfig(nameof<inputType>("loadBalancerScheme"), { value: args.loadBalancerScheme })
    if (args.ingressUseDedicatedMachines !== undefined) {
        await stack.setConfig(nameof<inputType>("ingressUseDedicatedMachines"), { value: JSON.stringify(args.ingressUseDedicatedMachines) })
    }
    if (args.ingressReplicas !== undefined) {
        await stack.setConfig(nameof<inputType>("ingressReplicas"), { value: String(args.ingressReplicas) })
    }
    await stack.setConfig(nameof<inputType>("clusterName"), { value: args.clusterName });
    await stack.setConfig(nameof<inputType>("nodeInstanceRoleArn"), { value: args.nodeInstanceRoleArn })

    await stack.setConfig(nameof<inputType>("glueSourceBucket"), { value: args.glueSourceBucket })
    await stack.setConfig(nameof<inputType>("glueSourceScript"), { value: args.glueSourceScript })
    await stack.setConfig(nameof<inputType>("glueTrainingDataBucket"), { value: args.glueTrainingDataBucket })

    await stack.setConfig(nameof<inputType>("offlineAggregateSourceBucket"), { value: args.offlineAggregateSourceBucket })
    await stack.setConfig(nameof<inputType>("offlineAggregateSourceFiles"), { value: JSON.stringify(args.offlineAggregateSourceFiles) })

    await stack.setConfig(nameof<inputType>("otelCollectorEndpoint"), { value: args.otelCollectorEndpoint })
    await stack.setConfig(nameof<inputType>("otelCollectorHttpEndpoint"), { value: args.otelCollectorHttpEndpoint })

    if (args.httpServerConf !== undefined) {
        await stack.setConfig(nameof<inputType>("httpServerConf"), { value: JSON.stringify(args.httpServerConf) })
    }

    if (args.queryServerConf !== undefined) {
        await stack.setConfig(nameof<inputType>("queryServerConf"), { value: JSON.stringify(args.queryServerConf) })
    }

    if (args.countAggrConf !== undefined) {
        await stack.setConfig(nameof<inputType>("countAggrConf"), { value: JSON.stringify(args.countAggrConf) })
    }

    if (args.counterCleanupConf !== undefined) {
        await stack.setConfig(nameof<inputType>("counterCleanupConf"), { value: JSON.stringify(args.counterCleanupConf) })
    }

    if (args.enableNitrous !== undefined) {
        await stack.setConfig(nameof<inputType>("enableNitrous"), { value: JSON.stringify(args.enableNitrous) })
    }

    if (args.sagemakerConf !== undefined) {
        await stack.setConfig(nameof<inputType>("sagemakerConf"), { value: JSON.stringify(args.sagemakerConf) })
    }

    await stack.setConfig(nameof<inputType>("nodeInstanceRole"), { value: args.nodeInstanceRole })

    await stack.setConfig(nameof<inputType>("vpcId"), { value: args.vpcId })
    await stack.setConfig(nameof<inputType>("connectedSecurityGroups"), { value: JSON.stringify(args.connectedSecurityGroups) })

    await stack.setConfig(nameof<inputType>("milvusEndpoint"), { value: args.milvusEndpoint })

    if (args.airbyteConf !== undefined) {
        await stack.setConfig(nameof<inputType>("airbyteConf"), { value: JSON.stringify(args.airbyteConf) })
    }

    if (args.plan !== undefined) {
        await stack.setConfig("plan", { value: String(args.plan) })
    }

    if (args.requestLimit !== undefined) {
        await stack.setConfig("requestLimit", { value: String(args.requestLimit) })
    }

    if (args.customerId !== undefined) {
        await stack.setConfig("customerId", { value: String(args.customerId) })
    }

    console.info("config set");

    if (preview) {
        console.info("previewing stack...");
        const previewRes = await stack.preview({ onOutput: console.info });
        console.info(previewRes);
        process.exit(0);
    }

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
