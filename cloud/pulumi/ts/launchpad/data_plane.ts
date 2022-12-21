import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import * as vpc from "../vpc";
import * as eks from "../eks";
import * as milvus from "../milvus";
import * as nitrous from "../nitrous";
import * as account from "../account";
import * as aurora from "../aurora";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as telemetry from "../telemetry";
import * as prometheus from "../prometheus";
import * as connectorSink from "../connectorsink";
import * as glueSource from "../glue-script-source";
import * as offlineAggregateSources from "../offline-aggregate-script-source";
import * as planeEksPermissions from "../plane-eks-permissions";
import * as postgres from "../postgres";
import * as modelMonitoring from "../model-monitoring";
import * as msk from "../msk";
import * as util from "../lib/util";

import * as process from "process";
import { OtelConfig } from "../telemetry";
import { Customer } from "../mothership-updates";

type VpcConfig = {
    cidr: string,
}


type MskConf = {
    // see valid values - https://aws.amazon.com/msk/pricing/
    brokerType: string,

    // this must be a multiple of the number of subnets in which the MSK cluster is being configured
    numberOfBrokerNodes: number,
    storageVolumeSizeGiB: number,
    // TODO(mohit): Consider adding support for volume throughput management
}

type RedisConfig = {
    numShards?: number,
    numReplicasPerShard?: number,
    nodeType?: string,
}

type CacheConfg = {
    nodeType?: string,
    numNodeGroups?: number,
    replicasPerNodeGroup?: number,
}

type PrometheusConf = {
    volumeSizeGiB?: number,
    metricsRetentionDays?: number,
    nodeSelector?: Record<string, string>,
}

type MilvusConf = {}

type ModelMonitoringConf = {}

type NitrousConf = {
    replicas?: number,
    useAmd64?: boolean,
    enforceReplicaIsolation?: boolean,
    resourceConf?: util.ResourceConf,
    nodeLabels?: Record<string, string>,
    storageClass: string
    storageCapacityGB: number
    forceLoadBackup?: boolean,
    binlog: nitrous.binlogConfig,
    mskBinlog?: nitrous.binlogConfig,
    backupConf?: nitrous.backupConf,
}

type NewAccount = {
    // account name
    name: string,
    // this is the email associated with this account, this should be unique i.e. an AWS account, even outside the
    // organization is not supposed to have used this email.
    //
    // consider using email of the form: `admin+{account-name}@fennel.ai` to easily map the accounts with the email
    email: string,
}

type ExistingAccount = {
    // ARN of the IAM role which has access in the existing account to create/update/delete the resources
    roleArn: string
}

// Account configuration
//
// Only one of them should be set
type AccountConf = {
    // Configuration for creating a new account to setup the plane
    newAccount?: NewAccount,
    // Configuration to use an existing account to setup the plane
    existingAccount?: ExistingAccount,
}



export type DataPlaneConf = {
    // Optional name of the plane - this is to disambiguate resources which exist in the global namespace
    // e.g. pulumi stack name, s3 buckets etc. If this is not set, we use the planeId configured (which is a required
    // field)
    planeName?: string,
    // Should be set to false, when deleting the plane
    //
    // Else, individual data storage resources, if they are to be deleted, should be set to false and the stack should
    // be updated
    //
    // NOTE: Please add a justification if this value is being set to False and the configuration is being checked-in
    protectResources: boolean,

    accountConf: AccountConf,

    planeId: number,
    region: string,
    vpcConf: VpcConfig,
    mskConf: MskConf,
    dbConf: util.DBConfig,
    controlPlaneConf: vpc.controlPlaneConfig,
    redisConf?: RedisConfig,
    cacheConf?: CacheConfg,
    otelConf?: OtelConfig,
    prometheusConf?: PrometheusConf,
    eksConf: util.EksConf,
    milvusConf?: MilvusConf,
    nitrousConf?: NitrousConf,
    // TODO(mohit): Make this default going forward
    modelMonitoringConf?: ModelMonitoringConf,
    // TODO(amit): Make this non optional going forward.
    mothershipId?: number,
    customer?: Customer,
}

export type PlaneOutput = {
    // ARN of the IAM role using which the resources were created in the plane and will be created in the tier
    roleArn: string,
    eks: eks.outputType,
    vpc: vpc.outputType,
    redis: redis.outputType,
    elasticache: elasticache.outputType,
    db: aurora.outputType,
    postgresDb: postgres.outputType,
    prometheus: prometheus.outputType,
    trainingData: connectorSink.outputType,
    offlineAggregateSourceFiles: offlineAggregateSources.outputType,
    glue: glueSource.outputType,
    telemetry: telemetry.outputType,
    milvus: milvus.outputType,
    msk?: msk.outputType,
    id: number,
    customerId: number,
    mothershipId: number,
    region: number,
    nitrous: nitrous.outputType,
}

const parseConfig = (): DataPlaneConf => {
    const config = new pulumi.Config();
    return config.requireObject("input");
};

const setupPlugins = async (stack: pulumi.automation.Stack) => {
    // TODO: aggregate plugins from all projects. If there are multiple versions
    // of the same plugin in different projects, we might want to use the latest.
    let plugins: { [key: string]: string } = {
        ...vpc.plugins,
        ...eks.plugins,
        ...aurora.plugins,
        ...elasticache.plugins,
        ...redis.plugins,
        ...telemetry.plugins,
        ...connectorSink.plugins,
        ...glueSource.plugins,
        ...offlineAggregateSources.plugins,
        ...milvus.plugins,
        ...postgres.plugins,
        ...nitrous.plugins,
        ...planeEksPermissions.plugins,
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
    // setup account for the plane, if configured explicitly. Else, use the master account.
    let roleArn: pulumi.Output<string>;
    if (input.accountConf.newAccount !== undefined) {
        const accountOutput = await account.setup({
            name: input.accountConf.newAccount.name,
            email: input.accountConf.newAccount.email
        })
        roleArn = accountOutput.roleArn;
    } else if (input.accountConf.existingAccount !== undefined) {
        roleArn = pulumi.output(input.accountConf.existingAccount.roleArn);
    } else {
        console.info("both newAccount and existingAccount are undefined; Exactly one of them should be set")
        process.exit(1)
    }

    const vpcOutput = await vpc.setup({
        region: input.region,
        roleArn: roleArn,
        cidr: input.vpcConf.cidr,
        controlPlane: input.controlPlaneConf,
        planeId: input.planeId,
    })
    const eksOutput = await eks.setup({
        roleArn: roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        publicSubnets: vpcOutput.publicSubnets,
        privateSubnets: vpcOutput.privateSubnets,
        connectedVpcCidrs: [input.controlPlaneConf.cidrBlock],
        planeId: input.planeId,
        nodeGroups: input.eksConf.nodeGroups,
        spotReschedulerConf: input.eksConf.spotReschedulerConf,
        scope: util.Scope.DATAPLANE,
    });
    const postgresDbOutput = await postgres.setup({
        roleArn: roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        minCapacity: 2,
        maxCapacity: 2,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        planeId: input.planeId,
        protect: input.protectResources,
    });
    const auroraOutput = await aurora.setup({
        roleArn: roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        minCapacity: input.dbConf.minCapacity || 1,
        maxCapacity: input.dbConf.maxCapacity || 1,
        username: "admin",
        password: pulumi.output(input.dbConf.password),
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        planeId: input.planeId,
        skipFinalSnapshot: input.dbConf.skipFinalSnapshot,
        protect: input.protectResources,
        scope: util.Scope.DATAPLANE,
    })
    const redisOutput = await redis.setup({
        roleArn: roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        numShards: input.redisConf?.numShards,
        numReplicasPerShard: input.redisConf?.numReplicasPerShard,
        nodeType: input.redisConf?.nodeType,
        connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        azs: vpcOutput.azs,
        planeId: input.planeId,
        protect: input.protectResources,
    })
    const elasticacheOutput = await elasticache.setup({
        roleArn: roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        planeId: input.planeId,
        nodeType: input.cacheConf?.nodeType,
        numNodeGroups: input.cacheConf?.numNodeGroups,
        replicasPerNodeGroup: input.cacheConf?.replicasPerNodeGroup,
        protect: input.protectResources,
    })
    let milvusOutput: milvus.outputType = {
        endpoint: ""
    };
    if (input.milvusConf !== undefined) {
        milvusOutput = await milvus.setup({
            region: input.region,
            roleArn: roleArn,
            planeId: input.planeId,
            planeName: input.planeName,
            kubeconfig: eksOutput.kubeconfig
        })
    }
    if (input.modelMonitoringConf !== undefined) {
        const modelMonitoringOutput = await modelMonitoring.setup({
            planeId: input.planeId,
            region: input.region,
            roleArn: roleArn,
            kubeconfig: eksOutput.kubeconfig
        });
    }
    const mskOutput = await msk.setup({
        planeId: input.planeId,
        planeName: input.planeName,
        region: input.region,
        roleArn: roleArn,
        privateSubnets: vpcOutput.privateSubnets,
        brokerType: input.mskConf.brokerType,
        numberOfBrokerNodes: input.mskConf.numberOfBrokerNodes,
        storageVolumeSizeGiB: input.mskConf.storageVolumeSizeGiB,
        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        protect: input.protectResources,
    });
    const connectorSinkOutput = await connectorSink.setup({
        region: input.region,
        roleArn: roleArn,
        planeId: input.planeId,
        planeName: input.planeName,
        protect: input.protectResources,
    })

    const prometheusOutput = await prometheus.setup({
        kubeconfig: eksOutput.kubeconfig,
        region: input.region,
        roleArn: roleArn,
        planeId: input.planeId,
        metricsRetentionDays: input.prometheusConf?.metricsRetentionDays,
        volumeSizeGiB: input.prometheusConf?.volumeSizeGiB,
        nodeSelector: input.prometheusConf?.nodeSelector,
        // set msk brokers, so that prometheus can scrape the metrics exported at each of the metrics
        mskBootstrapServers: mskOutput.bootstrapBrokers,
        numBrokers: mskOutput.numBrokers,
        protect: input.protectResources,
    })

    const telemetryOutput = await telemetry.setup({
        planeId: input.planeId,
        region: input.region,
        roleArn: roleArn,
        eksClusterName: eksOutput.clusterName,
        otelConf: input.otelConf,
        kubeconfig: eksOutput.kubeconfig,
        nodeInstanceRole: eksOutput.instanceRole,
    })

    let nitrousOutput: nitrous.outputType | undefined;
    if (input.nitrousConf !== undefined) {
        nitrousOutput = await nitrous.setup({
            planeId: input.planeId,
            planeName: input.planeName,

            region: input.region,
            roleArn: roleArn,
            nodeInstanceRole: eksOutput.instanceRole,
            kubeconfig: eksOutput.kubeconfig,
            otlpEndpoint: telemetryOutput.otelCollectorEndpoint,

            replicas: input.nitrousConf.replicas,
            useAmd64: input.nitrousConf.useAmd64,
            enforceReplicaIsolation: input.nitrousConf.enforceReplicaIsolation,
            resourceConf: input.nitrousConf.resourceConf,
            nodeLabels: input.nitrousConf.nodeLabels,

            storageCapacityGB: input.nitrousConf.storageCapacityGB,
            storageClass: eksOutput.storageclasses[input.nitrousConf.storageClass],

            forceLoadBackup: input.nitrousConf.forceLoadBackup,

            kafka: {
                username: mskOutput.mskUsername,
                password: mskOutput.mskPassword,
                bootstrapServers: mskOutput.bootstrapBrokers,
            },

            binlog: {
                partitions: input.nitrousConf.binlog.partitions,
                replicationFactor: input.nitrousConf.binlog.replicationFactor,
                retention_ms: input.nitrousConf.binlog.retention_ms,
                partition_retention_bytes: input.nitrousConf.binlog.partition_retention_bytes,
                max_message_bytes: input.nitrousConf.binlog.max_message_bytes,
            },

            backupConf: input.nitrousConf.backupConf,

            protect: input.protectResources,
        })
    }

    const offlineAggregateSourceFiles = await offlineAggregateSources.setup({
        region: input.region,
        roleArn: roleArn,
        planeId: input.planeId,
        planeName: input.planeName,
        protect: input.protectResources,
    })

    const glueOutput = await glueSource.setup({
        region: input.region,
        roleArn: roleArn,
        planeId: input.planeId,
        planeName: input.planeName,
        protect: input.protectResources,
    })

    await planeEksPermissions.setup({
        region: input.region,
        roleArn: roleArn,
        nodeInstanceRole: eksOutput.instanceRole,
    });

    return {
        roleArn: roleArn,
        eks: eksOutput,
        vpc: vpcOutput,
        redis: redisOutput,
        elasticache: elasticacheOutput,
        db: auroraOutput,
        postgresDb: postgresDbOutput,
        trainingData: connectorSinkOutput,
        offlineAggregateSourceFiles: offlineAggregateSourceFiles,
        glue: glueOutput,
        telemetry: telemetryOutput,
        milvus: milvusOutput,
        msk: mskOutput,
        id: input.planeId,
        mothershipId: input.mothershipId,
        customerId: input.customer === undefined ? 0 : input.customer.id,
        region: input.region,
        nitrous: nitrousOutput,
        prometheus: prometheusOutput,
    }
};

const setupDataPlane = async (args: DataPlaneConf, preview?: boolean, destroy?: boolean) => {
    const projectName = `launchpad`
    let stackName;
    if (args.planeName) {
        stackName = `fennel/${projectName}/${args.planeName}`
    } else {
        stackName = `fennel/${projectName}/plane-${args.planeId}`
    }

    console.log("Updating data plane: ", stackName);

    // validate that exactly one account configuration is set
    if (args.accountConf.newAccount !== undefined && args.accountConf.existingAccount !== undefined) {
        console.info("both newAccount and existingAccount configuration is set; Exactly one should be set")
        process.exit(1);
    }

    if (args.accountConf.newAccount === undefined && args.accountConf.existingAccount === undefined) {
        console.info("neither newAccount or existingAccount is set; Exactly one should be set")
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
    const stack = await LocalWorkspace.createOrSelectStack(stackArgs, {
        envVars: {
            "TF_LOG": "DEBUG"
        }
    });
    console.info("successfully initialized stack");

    await setupPlugins(stack)

    console.info("setting up config");

    await stack.setConfig("input", { value: JSON.stringify(args) })
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
    return upRes.outputs
};

export default setupDataPlane