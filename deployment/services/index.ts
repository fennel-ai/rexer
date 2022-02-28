import { LocalProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as upath from "upath";

const process = require('process');

const stacks = ["fennel/dev", "fennel/trell-demo", "fennel/lokal-demo", "fennel/t-102"] as const;
type Stack = typeof stacks[number];


async function updateHttpServer(stackName: string) {
    // Create our stack using the existing http-server stack.
    const args: LocalProgramArgs = {
        stackName,
        workDir: upath.joinSafe(process.env.FENNEL_ROOT, "cloud/pulumi/ts/http-server"),
    };
    const stack = await LocalWorkspace.selectStack(args);
    console.info("successfully initialized stack");
    console.info("refreshing stack...");
    await stack.refresh({ onOutput: console.info });
    console.info("refresh complete");
    console.info("updating stack...");
    const upRes = await stack.up({ onOutput: console.info });
    console.log(`update summary: \n${JSON.stringify(upRes.summary.resourceChanges, null, 4)}`);
}

async function updateAggregator(stackName: string) {
    // Create our stack using the existing countaggr stack.
    const args: LocalProgramArgs = {
        stackName,
        workDir: upath.joinSafe(process.env.FENNEL_ROOT, "cloud/pulumi/ts/countaggr"),
    };
    const stack = await LocalWorkspace.selectStack(args);
    console.info("successfully initialized stack");
    console.info("refreshing stack...");
    await stack.refresh({ onOutput: console.info });
    console.info("refresh complete");
    console.info("updating stack...");
    const upRes = await stack.up({ onOutput: console.info });
    console.log(`update summary: \n${JSON.stringify(upRes.summary.resourceChanges, null, 4)}`);
}

const run = async () => {
    let stackNames: Stack[] = [];
    const args: string = process.argv.slice(2);
    if (args.length > 0 && args[0]) {
        stackNames.push(args[0] as Stack);
    } else {
        stacks.forEach(s => stackNames.push(s));
    }
    stackNames.map(async stack => {
        await Promise.all([updateHttpServer(stack), updateAggregator(stack)]);
    })
};

run().catch(err => console.error(err));