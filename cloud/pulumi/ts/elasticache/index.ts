import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

import { fennelStdTags } from "../lib/util";

export const plugins = {
    "aws": "v5.0.0"
}

export type inputType = {
    roleArn: pulumi.Input<string>,
    region: string,
    vpcId: pulumi.Output<string>,
    connectedSecurityGroups: Record<string, pulumi.Output<string>>,
    connectedCidrBlocks?: string[],
    planeId: number,
    nodeType?: string,
    numNodeGroups?: number,
    replicasPerNodeGroup?: number,
    protect: boolean,
}

export type outputType = {
    "endpoint": string,
}

const REDIS_VERSION = "6.x";
const REDIS_FAMILY = "redis6.x";
// https://docs.aws.amazon.com/whitepapers/latest/database-caching-strategies-using-redis/evictions.html
//
// The cache evicts the least recently used (LRU) keys regardless of TTL set.
const DEFAULT_EVICTION_POLICY = "allkeys-lru";
const NODE_TYPE = "cache.t4g.micro";
const DEFAULT_NODE_GROUPS = 1;
const DEFAULT_REPLICAS_PER_NODE_GROUPS = 0;

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {

    const provider = new aws.Provider("cache-aws-provider", {
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

    const subnetGroup = new aws.elasticache.SubnetGroup(`p-${input.planeId}-cache-subnets`, {
        subnetIds: subnetIds.ids,
        tags: { ...fennelStdTags },
    }, { provider })

    const cacheSg = new aws.ec2.SecurityGroup(`p-${input.planeId}-cache-sg`, {
        vpcId: input.vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-ec-allow-${key}`, {
            securityGroupId: cacheSg.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }
    if (input.connectedCidrBlocks !== undefined) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-cache-allow-connected-cidr`, {
            securityGroupId: cacheSg.id,
            cidrBlocks: input.connectedCidrBlocks,
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    const parameterGroup = new aws.elasticache.ParameterGroup(`p-${input.planeId}-cache-pg`, {
        family: REDIS_FAMILY,
        parameters: [{
            name: "maxmemory-policy",
            value: DEFAULT_EVICTION_POLICY,
        }, {
            name: "cluster-enabled",
            value: "yes",
        }],
        tags: fennelStdTags,
    }, {provider})

    const cluster = new aws.elasticache.ReplicationGroup(`p-${input.planeId}-cache-cluster`, {
        // Apply any changes proposed immediately instead of applying them during maintenance window
        applyImmediately: true,
        // "redis" is optional here and also the only allowed value, but we
        // set it here anyway to be explicit.
        engine: "redis",
        engineVersion: REDIS_VERSION,
        replicationGroupDescription: "redis-based elastic cache",
        nodeType: input.nodeType || NODE_TYPE,
        securityGroupIds: [cacheSg.id],
        subnetGroupName: subnetGroup.name,
        transitEncryptionEnabled: true,
        atRestEncryptionEnabled: true,
        clusterMode: {
            numNodeGroups: input.numNodeGroups || DEFAULT_NODE_GROUPS,
            replicasPerNodeGroup: input.replicasPerNodeGroup !== undefined ? input.replicasPerNodeGroup : DEFAULT_REPLICAS_PER_NODE_GROUPS,
        },
        automaticFailoverEnabled: true,
        tags: { ...fennelStdTags },
        parameterGroupName: parameterGroup.name,
    }, { provider, protect: input.protect })

    const primaryAddress = cluster.primaryEndpointAddress
    const replicaAddress = cluster.readerEndpointAddress

    const endpoint = pulumi.all([cluster.configurationEndpointAddress, cluster.port]).apply(([address, port]) => {
        return `${address}:${port}`
    })

    const output = pulumi.output({
        endpoint
    })

    return output
}
