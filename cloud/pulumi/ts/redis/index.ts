import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

import { fennelStdTags } from "../lib/util";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    roleArn: pulumi.Input<string>,
    region: string,
    vpcId: pulumi.Output<string>,
    numShards?: number,
    numReplicasPerShard?: number,
    nodeType?: string,
    azs: pulumi.Output<string[]>,
    connectedSecurityGroups: Record<string, pulumi.Output<string>>,
    connectedCidrBlocks?: string[],
    planeId: number,
    protect: boolean,
}

export type outputType = {
    clusterId: string,
    clusterEndPoints: string[],
    clusterSecurityGroupIds?: string[],
}

const DEFAULT_REDIS_VERSION = "6.2";
const DEFAULT_NODE_TYPE = "db.t4g.small";
const DEFAULT_NUM_SHARDS = 1;
const DEFAULT_NUM_REPLICAS_PER_SHARD = 0;

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("redis-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const subnetIds = input.vpcId.apply(async vpcId => {
        return await aws.ec2.getSubnetIds({
            vpcId: vpcId,
            // TODO: use better method for filtering private subnets.
            filters: [{
                name: "tag:Name",
                values: [`p-${input.planeId}-primary-private-subnet`, `p-${input.planeId}-secondary-private-subnet`],
            }]
        }, { provider })
    })

    const subnetGroup = new aws.memorydb.SubnetGroup(`p-${input.planeId}-redis-subnet-group`, {
        subnetIds: subnetIds.ids,
        tags: { ...fennelStdTags },
    }, { provider })

    const redisSg = new aws.ec2.SecurityGroup(`p-${input.planeId}-redis-sg`, {
        namePrefix: `p-${input.planeId}-redis-sg-`,
        vpcId: input.vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-redis-allow-${key}`, {
            securityGroupId: redisSg.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }
    if (input.connectedCidrBlocks !== undefined) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-redis-allow-connected-cidr`, {
            securityGroupId: redisSg.id,
            cidrBlocks: input.connectedCidrBlocks,
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    const cluster = new aws.memorydb.Cluster(`p-${input.planeId}-redis-db`, {
        subnetGroupName: subnetGroup.id,
        aclName: "open-access",
        engineVersion: DEFAULT_REDIS_VERSION,
        autoMinorVersionUpgrade: true,
        tlsEnabled: true,
        securityGroupIds: [redisSg.id],
        numShards: input.numShards || DEFAULT_NUM_SHARDS,
        numReplicasPerShard: input.numReplicasPerShard || DEFAULT_NUM_REPLICAS_PER_SHARD,
        nodeType: input.nodeType || DEFAULT_NODE_TYPE,
        tags: { ...fennelStdTags },
    }, { provider, protect: input.protect })

    const output = pulumi.output({
        clusterId: cluster.id,
        clusterEndPoints: cluster.clusterEndpoints.apply(endpoints => endpoints.map(endpoint => `${endpoint.address}:${endpoint.port}`)),
        clusterSecurityGroupIds: cluster.securityGroupIds,
    })

    return output
}
