import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as pulumi from "@pulumi/pulumi"

import { nameof } from "../lib/util"

import * as process from "process";

type inputType = {
    planeId: number
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        planeId: Number(config.require(nameof<inputType>("planeId"))),
    };
};

const setupPlugins = async (stack: pulumi.automation.Stack) => {
    // TODO: aggregate plugins from all projects. If there are multiple versions
    // of the same plugin in different projects, we might want to use the latest.
    let plugins: { [key: string]: string } = {
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
};

type PlaneConf = {
    planeId: string,
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
