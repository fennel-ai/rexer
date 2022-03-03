import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import * as vpc from "../vpc";
import * as eks from "../eks";
import * as aurora from "../aurora";
import * as elasticache from "../elasticache";
import * as redis from "../redis";
import * as confluentenv from "../confluentenv";
import * as telemetry from "../telemetry";

import { nameof } from "../lib/util"

import * as process from "process";

type VpcConfig = {
    cidr: string,
}

type DBConfig = {
    minCapacity: number,
    maxCapacity: number,
    password: string,
}

type ConfluentConfig = {
    username: string,
    password: string,
}

type PlaneConf = {
    planeId: number,
    region: string,
    roleArn: string,
    vpcConf: VpcConfig,
    dbConf: DBConfig,
    confluentConf: ConfluentConfig,
    controlPlaneConf: vpc.controlPlaneConfig,
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
    })
    const eksOutput = vpcOutput.vpcId.apply(async vpcId => {
        return eks.setup({
            roleArn: input.roleArn,
            region: input.region,
            vpcId: vpcId,
            connectedVpcCidrs: [input.controlPlaneConf.cidrBlock],
        })
    })
    const auroraOutput = pulumi.all([vpcOutput, eksOutput]).apply(async ([vpc, eks]) => {
        return aurora.setup({
            roleArn: input.roleArn,
            region: input.region,
            vpcId: vpc.vpcId,
            minCapacity: input.dbConf.minCapacity,
            maxCapacity: input.dbConf.maxCapacity,
            username: "admin",
            password: pulumi.output(input.dbConf.password),
            connectedSecurityGroups: {
                "eks": eks.workerSg,
            },
            connectedCidrBlocks: [input.controlPlaneConf.cidrBlock],
        })
    })
    const redisOutput = pulumi.all([vpcOutput, eksOutput]).apply(async ([vpc, eks]) => {
        return redis.setup({
            roleArn: input.roleArn,
            region: input.region,
            vpcId: vpc.vpcId,
            connectedSecurityGroups: {
                "eks": eks.workerSg,
            },
            azs: vpc.azs,
        })
    })
    const elasticacheOutput = pulumi.all([vpcOutput, eksOutput]).apply(async ([vpc, eks]) => {
        return elasticache.setup({
            roleArn: input.roleArn,
            region: input.region,
            vpcId: vpc.vpcId,
            azs: vpc.azs,
            connectedSecurityGroups: {
                "eks": eks.workerSg,
            }
        })
    })
    const confluentOutput = await confluentenv.setup({
        region: input.region,
        username: input.confluentConf.username,
        password: pulumi.output(input.confluentConf.password),
        envName: `plane-${input.planeId}`,
    })
    const telemetryOutput = pulumi.all([eksOutput.clusterName, eksOutput.kubeconfig, eksOutput.instanceRole]).apply(
        async ([eksClusterName, kubeconfig, nodeInstanceRole]) => {
            return telemetry.setup({
                region: input.region,
                roleArn: input.roleArn,
                eksClusterName,
                kubeconfig,
                nodeInstanceRole,
            })
        })
};

const setupDataPlane = async (args: PlaneConf, destroy?: boolean) => {
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

export default setupDataPlane
