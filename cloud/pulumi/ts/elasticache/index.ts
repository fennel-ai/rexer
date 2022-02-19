import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as process from "process";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "aws": "v4.37.5"
}

export type inputType = {
    roleArn: string,
    region: string,
    vpcId: string,
    azs: string[],
    eksSecurityGroup: string,
}

export type outputType = {
    cacheNodes: pulumi.Output<cacheNode[]>,
}

const REDIS_VERSION = "6.x";
const NODE_TYPE = "cache.t4g.medium";
// TODO: Increase replica count once we add more than one subnet to group.
const NUM_REPLICAS = 0;

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        vpcId: config.require(nameof<inputType>("vpcId")),
        azs: config.requireObject(nameof<inputType>("azs")),
        eksSecurityGroup: config.require(nameof<inputType>("eksSecurityGroup")),
    }
}

type cacheNode = {
    availabilityZone: string
    address: string
}

export const setup = async (input: inputType) => {

    const provider = new aws.Provider("aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const subnetIds = await aws.ec2.getSubnetIds({
        vpcId: input.vpcId,
        // TODO: use better method for filtering private subnets.
        filters: [{
            name: "tag:Name",
            values: ["fennel-primary-private-subnet", "fennel-secondary-private-subnet"],
        }]
    }, { provider })

    const subnetGroup = new aws.elasticache.SubnetGroup("cache-subnets", {
        subnetIds: subnetIds.ids,
        tags: { ...fennelStdTags },
    }, { provider })

    const cacheSg = new aws.ec2.SecurityGroup("cache-sg", {
        vpcId: input.vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    const allowEksTraffic = new aws.ec2.SecurityGroupRule("allow-eks", {
        securityGroupId: cacheSg.id,
        sourceSecurityGroupId: input.eksSecurityGroup,
        fromPort: 0,
        toPort: 65535,
        type: "ingress",
        protocol: "tcp",
    }, { provider })

    const cluster = new aws.elasticache.Cluster("cache-cluster", {
        subnetGroupName: subnetGroup.name,
        securityGroupIds: [cacheSg.id],
        engine: "redis",
        engineVersion: REDIS_VERSION,
        nodeType: NODE_TYPE,
        preferredAvailabilityZones: input.azs,
        numCacheNodes: 1,
        tags: { ...fennelStdTags },
    }, { provider })

    const cacheNodes: pulumi.Output<cacheNode[]> = cluster.cacheNodes.apply(nodes => {
        return nodes.map(node => {
            return {
                availabilityZone: node.availabilityZone,
                address: `${node.address}:${node.port}`
            }
        })
    })

    const output: outputType = {
        cacheNodes,
    }
    return output
}

async function run() {
    let output: outputType | undefined;
    // Run the main function only if this program is run through the pulumi CLI.
    // Unfortunately, in that case the argv0 itself is not "pulumi", but the full
    // path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
    if (process.argv0 !== 'node') {
        pulumi.log.info("Running...")
        const input: inputType = parseConfig();
        output = await setup(input)
    }
    return output
}


export const output = await run();