import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import * as vpc from "../vpc";
import * as eks from "../eks";

import { nameof } from "../lib/util"

import * as process from "process";

type inputType = {
    planeId: number,
    // vpc configuration.
    cidr: string,
    region: string,
    roleArn: string,
    // control plane configuration.
    controlPlaneConfig: vpc.controlPlaneConfig,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        planeId: Number(config.require(nameof<inputType>("planeId"))),
        cidr: config.require(nameof<inputType>("cidr")),
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        controlPlaneConfig: config.requireObject(nameof<inputType>("controlPlaneConfig")),
    };
};

const setupPlugins = async (stack: pulumi.automation.Stack) => {
    // TODO: aggregate plugins from all projects. If there are multiple versions
    // of the same plugin in different projects, we might want to use the latest.
    let plugins: { [key: string]: string } = {
        ...vpc.plugins,
        ...eks.plugins,
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
        cidr: input.cidr,
        region: input.region,
        roleArn: input.roleArn,
        controlPlane: input.controlPlaneConfig,
    })
    const eksOutput = vpcOutput.vpcId.apply(async vpcId => {
        await eks.setup({
            roleArn: input.roleArn,
            region: input.region,
            vpcId: vpcId,
            connectedVpcCidrs: [input.controlPlaneConfig.cidrBlock],
        })
    })
};

type PlaneConf = {
    planeId: number,
    // vpc configuration.
    cidr: string,
    region: string,
    roleArn: string,
    // control plane configuration.
    controlPlaneConfig: vpc.controlPlaneConfig,
}

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

    await stack.setConfig(nameof<inputType>("planeId"), { value: String(args.planeId) })
    await stack.setConfig(nameof<inputType>("cidr"), { value: args.cidr })
    await stack.setConfig(nameof<inputType>("region"), { value: args.region })
    await stack.setConfig(nameof<inputType>("roleArn"), { value: args.roleArn })
    await stack.setConfig(nameof<inputType>("controlPlaneConfig"), { value: JSON.stringify(args.controlPlaneConfig) })

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
