import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import * as vpc from "../vpc";
import * as eks from "../eks";
import * as milvus from "../milvus";
import * as aurora from "../aurora";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";
import * as telemetry from "../telemetry";
import * as prometheus from "../prometheus";
import * as connectorSink from "../connectorsink";
import * as glueSource from "../glue-script-source";
import * as offlineAggregateSources from "../offline-aggregate-script-source";

import * as process from "process";
import {NodeGroupConf} from "../eks";

type VpcConfig = {
    cidr: string,
}

type DBConfig = {
    minCapacity?: number
    maxCapacity?: number,
    password: string,
    skipFinalSnapshot: boolean,
}

type ConfluentConfig = {
    username: string,
    password: string,
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
    // This should be set to `true` if Amazon Managed Prometheus (AMP) should be
    // used to store metrics.
    //
    // This should be eventually removed and assumed `true` by default.
    // Currently AMP is not available in ap-sount-1 where we have data planes.
    useAMP: boolean
}

type EksConf = {
    // EKS cluster can have more than one Node Group
    nodeGroups: NodeGroupConf[]
}

type MilvusConf = {}

export type PlaneConf = {
    planeId: number,
    region: string,
    roleArn: string,
    vpcConf: VpcConfig,
    dbConf: DBConfig,
    confluentConf: ConfluentConfig,
    controlPlaneConf: vpc.controlPlaneConfig,
    redisConf?: RedisConfig,
    cacheConf?: CacheConfg,
    prometheusConf: PrometheusConf,
    eksConf?: EksConf,
    milvusConf?: MilvusConf,
}

export type PlaneOutput = {
    eks: eks.outputType,
    vpc: vpc.outputType,
    redis: redis.outputType,
    elasticache: elasticache.outputType,
    confluent: confluentenv.outputType,
    db: aurora.outputType,
    prometheus: prometheus.outputType,
    trainingData: connectorSink.outputType,
    offlineAggregateSourceFiles: offlineAggregateSources.outputType,
    glue: glueSource.outputType,
}

const parseConfig = (): PlaneConf => {
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
        ...confluentenv.plugins,
        ...telemetry.plugins,
        ...connectorSink.plugins,
        ...glueSource.plugins,
        ...offlineAggregateSources.plugins,
        ...milvus.plugins,
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
    const vpcOutput = await vpc.setup({
        region: input.region,
        roleArn: input.roleArn,
        cidr: input.vpcConf.cidr,
        controlPlane: input.controlPlaneConf,
        planeId: input.planeId,
    })
    const eksOutput = await eks.setup({
        roleArn: input.roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        connectedVpcCidrs: [input.controlPlaneConf.cidrBlock],
        planeId: input.planeId,
        nodeGroups: input.eksConf?.nodeGroups,
    })
    const auroraOutput = await aurora.setup({
        roleArn: input.roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        minCapacity: input.dbConf.minCapacity || 1,
        maxCapacity: input.dbConf.maxCapacity || 1,
        username: "admin",
        password: pulumi.output(input.dbConf.password),
        connectedSecurityGroups: {
            "eks": eksOutput.workerSg,
        },
        connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        planeId: input.planeId,
        skipFinalSnapshot: input.dbConf.skipFinalSnapshot,
    })
    const redisOutput = await redis.setup({
        roleArn: input.roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.workerSg,
        },
        numShards: input.redisConf?.numShards,
        numReplicasPerShard: input.redisConf?.numReplicasPerShard,
        nodeType: input.redisConf?.nodeType,
        azs: vpcOutput.azs,
        planeId: input.planeId,
    })
    const elasticacheOutput = await elasticache.setup({
        roleArn: input.roleArn,
        region: input.region,
        vpcId: vpcOutput.vpcId,
        connectedSecurityGroups: {
            "eks": eksOutput.workerSg,
        },
        planeId: input.planeId,
        nodeType: input.cacheConf?.nodeType,
        numNodeGroups: input.cacheConf?.numNodeGroups,
        replicasPerNodeGroup: input.cacheConf?.replicasPerNodeGroup,
    })
    if (input.milvusConf !== undefined) {
        const milvusOutput = await milvus.setup({
            region: input.region,
            roleArn: input.roleArn,
            planeId: input.planeId,
            kubeconfig: eksOutput.kubeconfig,
        })
    }
    const confluentOutput = await confluentenv.setup({
        region: input.region,
        username: input.confluentConf.username,
        password: pulumi.output(input.confluentConf.password),
        envName: `plane-${input.planeId}`,
    })
    const connectorSinkOutput = await connectorSink.setup({
        region: input.region,
        roleArn: input.roleArn,
        planeId: input.planeId
    })

    const prometheusOutput = await prometheus.setup({
        useAMP: input.prometheusConf.useAMP,
        kubeconfig: eksOutput.kubeconfig,
        region: input.region,
        roleArn: input.roleArn,
        planeId: input.planeId,
    })

    const telemetryOutput = await telemetry.setup({
        planeId: input.planeId,
        region: input.region,
        roleArn: input.roleArn,
        eksClusterName: eksOutput.clusterName,
        kubeconfig: eksOutput.kubeconfig,
        nodeInstanceRole: eksOutput.instanceRole,
        prometheusEndpoint: prometheusOutput.prometheusWriteEndpoint,
    })

    const offlineAggregateSourceFiles = await offlineAggregateSources.setup({
        region: input.region,
        roleArn: input.roleArn,
        planeId: input.planeId,
    })

    const glueOutput = await glueSource.setup({
        region: input.region,
        roleArn: input.roleArn,
        planeId: input.planeId,
    })

    return {
        eks: eksOutput,
        vpc: vpcOutput,
        redis: redisOutput,
        elasticache: elasticacheOutput,
        confluent: confluentOutput,
        db: auroraOutput,
        prometheus: prometheusOutput,
        trainingData: connectorSinkOutput,
        offlineAggregateSourceFiles: offlineAggregateSourceFiles,
        glue: glueOutput,
    }
};

const setupDataPlane = async (args: PlaneConf, preview?: boolean, destroy?: boolean) => {
    const projectName = `launchpad`
    const stackName = `fennel/${projectName}/plane-${args.planeId}`

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
