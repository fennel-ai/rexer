import { InlineProgramArgs, LocalWorkspace } from "@pulumi/pulumi/automation";
import * as util from "../lib/util"
import * as vpc from "../vpc"
import * as pulumi from "@pulumi/pulumi"
import * as mysql from "../mysql";
import * as ns from "../k8s-ns";
import * as eks from "../eks";
import * as aurora from "../aurora";
import * as ingress from "../ingress";
import * as cert from "../cert";
import { MASTER_ACCOUNT_ADMIN_ROLE_ARN } from "../account";
import * as bridgeserver from "../bridge";
import * as mothershipconfigs from "../mothership-configs"
import * as k8s from "@pulumi/kubernetes";


export type BridgeServerConf = {
    podConf?: util.PodConf,
    envVars?: pulumi.Input<k8s.types.input.core.v1.EnvVar>[],
}

export type MothershipConf = {
    // Should be set to false, when deleting the plane
    //
    // Else, individual data storage resources, if they are to be deleted, should be set to false and the stack should
    // be updated
    //
    // NOTE: Please add a justification if this value is being set to False and the configuration is being checked-in
    protectResources: boolean,
    dbConf: util.DBConfig,
    eksConf: util.EksConf,
    planeId: number,
    planeName?: string,
    vpcConf: vpc.controlPlaneConfig,
    ingressConf?: util.IngressConf,
    bridgeServerConf?: BridgeServerConf,
    dnsName?: string,
}

const parseConfig = (): MothershipConf => {
    const config = new pulumi.Config();
    return config.requireObject("input");
}

const setupPlugins = async (stack: pulumi.automation.Stack) => {
    // TODO: aggregate plugins from all projects. If there are multiple versions
    // of the same plugin in different projects, we might want to use the latest.
    let plugins: { [key: string]: string } = {
        ...vpc.plugins,
        ...eks.plugins,
        ...aurora.plugins,
        ...mothershipconfigs.plugins,
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

    const eksOutput = await eks.setup({
        roleArn: input.vpcConf.roleArn,
        region: input.vpcConf.region,
        vpcId: pulumi.output(input.vpcConf.vpcId),
        publicSubnets: pulumi.output([input.vpcConf.primaryPublicSubnet, input.vpcConf.secondaryPublicSubnet]),
        privateSubnets: pulumi.output([input.vpcConf.primaryPrivateSubnet, input.vpcConf.secondaryPrivateSubnet]),
        planeId: input.planeId,
        nodeGroups: input.eksConf.nodeGroups,
        spotReschedulerConf: input.eksConf.spotReschedulerConf,
        scope: util.Scope.MOTHERSHIP,
    });
    const auroraOutput = await aurora.setup({
        roleArn: input.vpcConf.roleArn,
        region: input.vpcConf.region,
        vpcId: pulumi.output(input.vpcConf.vpcId),
        minCapacity: input.dbConf.minCapacity || 1,
        maxCapacity: input.dbConf.maxCapacity || 1,
        username: "admin",
        password: pulumi.output(input.dbConf.password),
        connectedSecurityGroups: {
            "eks": eksOutput.clusterSg,
        },
        planeId: input.planeId,
        // disable auto-pausing the DB instance to avoid long waits on webapp login
        autoPause: false,
        skipFinalSnapshot: input.dbConf.skipFinalSnapshot,
        connectedCidrBlocks: [input.vpcConf.cidrBlock],
        protect: input.protectResources,
        scope: util.Scope.MOTHERSHIP,
    })
    // setup mysql db.
    // Comment this when direct connection to the db instance is not possible.
    // This will usually be when trying to setup a tier in a customer vpc, which
    // should usually be done through the bridge.
    const dbName = `${util.getPrefix(util.Scope.MOTHERSHIP, input.planeId)}-db`
    const dbUser = "admin"
    const sqlDB = await mysql.setup({
        username: dbUser,
        password: pulumi.output(input.dbConf.password),
        endpoint: auroraOutput.host,
        db: dbName,
        protect: input.protectResources,
    })
    const kconf = pulumi.all([eksOutput.kubeconfig]).apply(([k]) => JSON.stringify(k))
    console.log("kubernetes config: ", kconf)
    const nsName = `${util.getPrefix(util.Scope.MOTHERSHIP, input.planeId)}`
    // setup k8s namespaces.
    const namespace = await ns.setup({
        namespace: nsName,
        kubeconfig: kconf,
    })
    // setup ingress.

    const ingressOutput = await ingress.setup({
        scopeId: input.planeId,
        roleArn: input.vpcConf.roleArn,
        region: input.vpcConf.region,
        kubeconfig: kconf,
        namespace: nsName,
        privateSubnetIds: [input.vpcConf.primaryPrivateSubnet, input.vpcConf.secondaryPrivateSubnet],
        publicSubnetIds: [input.vpcConf.primaryPublicSubnet, input.vpcConf.secondaryPublicSubnet],
        ingressConf: input.ingressConf,
        clusterName: eksOutput.clusterName,
        nodeRoleArn: eksOutput.instanceRoleArn,
        scope: util.Scope.MOTHERSHIP,
        // Got this list from https://www.cloudflare.com/ips-v4 and
        // https://www.cloudflare.com/ips-v6.
        loadBalancerSourceIpRanges: [
            "173.245.48.0/20",
            "103.21.244.0/22",
            "103.22.200.0/22",
            "103.31.4.0/22",
            "141.101.64.0/18",
            "108.162.192.0/18",
            "190.93.240.0/20",
            "188.114.96.0/20",
            "197.234.240.0/22",
            "198.41.128.0/17",
            "162.158.0.0/15",
            "104.16.0.0/13",
            "104.24.0.0/14",
            "172.64.0.0/13",
            "131.0.72.0/22",
            "2400:cb00::/32",
            "2606:4700::/32",
            "2803:f800::/32",
            "2405:b500::/32",
            "2405:8100::/32",
            "2a06:98c0::/29",
            "2c0f:f248::/32",
        ],
    })
    // setup configs after resources are setup.
    const configsOutput = pulumi.all(
        [input.dbConf.password, auroraOutput]).apply(async ([dbPassword, auroraOutput]) => {
            return await ingressOutput.apply(async ingress => {
                return await mothershipconfigs.setup({
                    kubeconfig: kconf,
                    namespace: nsName,
                    mothershipConfig: {
                        "mothership_id": String(input.planeId),
                        "mothership_endpoint": input.dnsName === undefined ? `http://${ingress.loadBalancerUrl}` : `https://${input.dnsName}`,
                        "gin_mode": "release",
                    },
                    dbConfig: pulumi.output({
                        "host": auroraOutput.host,
                        "db": dbName,
                        "username": dbUser,
                        "password": dbPassword,
                    } as Record<string, string>),
                })
            })
        })
    configsOutput.apply(async () => {
        if (input.bridgeServerConf !== undefined) {
            var certOut = undefined
            if (input.dnsName !== undefined) {
                certOut = await cert.setup({
                    kubeconfig: kconf,
                    scopeId: input.planeId,
                    scope: util.Scope.MOTHERSHIP,
                    dnsName: input.dnsName,
                    namespace: nsName,
                })
            }

            await bridgeserver.setup({
                roleArn: input.vpcConf.roleArn,
                region: input.vpcConf.region,
                kubeconfig: kconf,
                namespace: nsName,
                mothershipId: input.planeId,
                minReplicas: input.bridgeServerConf?.podConf?.minReplicas,
                maxReplicas: input.bridgeServerConf?.podConf?.maxReplicas,
                resourceConf: input.bridgeServerConf?.podConf?.resourceConf,
                useAmd64: input.bridgeServerConf?.podConf?.useAmd64,
                nodeLabels: input.bridgeServerConf?.podConf?.nodeLabels,
                pprofHeapAllocThresholdMegaBytes: input.bridgeServerConf?.podConf?.pprofHeapAllocThresholdMegaBytes,
                tlsCertK8sSecretName: certOut !== undefined ? certOut.tlsCertK8sSecretName : ingressOutput.tlsK8sSecretRef,
                envVars: input.bridgeServerConf.envVars,
            });

        }

    })

    return {
        eks: eksOutput,
        db: {
            host: auroraOutput.host,
            user: auroraOutput.user,
            password: auroraOutput.password,
            dbName: dbName,
        },
        ingress: ingressOutput,
    }
};


const setupMothership = async (args: MothershipConf, preview?: boolean, destroy?: boolean) => {
    const projectName = `launchpad`
    const stackName = `fennel/${projectName}/mothership-${args.planeId}`

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
    const upRes = await stack.up({ onOutput: console.info, targetDependents: true });
    console.log(upRes)
    return upRes.outputs
};

export default setupMothership
