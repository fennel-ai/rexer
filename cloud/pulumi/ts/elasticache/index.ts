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
    "aws": "v4.38.0"
}

export type inputType = {
    roleArn: string,
    region: string,
    vpcId: pulumi.Output<string>,
    connectedSecurityGroups: Record<string, pulumi.Output<string>>,
}

export type outputType = {
    primaryAddress: pulumi.Output<string>,
    replicaAddress: pulumi.Output<string>,
}

const REDIS_VERSION = "6.x";
const NODE_TYPE = "cache.t4g.medium";

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        vpcId: pulumi.output(config.require(nameof<inputType>("vpcId"))),
        connectedSecurityGroups: config.requireObject(nameof<inputType>("connectedSecurityGroups")),
    }
}

export const setup = async (input: inputType) => {

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
                values: ["fennel-primary-private-subnet", "fennel-secondary-private-subnet"],
            }]
        }, { provider })
    })

    const subnetGroup = new aws.elasticache.SubnetGroup("cache-subnets", {
        subnetIds: subnetIds.ids,
        tags: { ...fennelStdTags },
    }, { provider })

    const cacheSg = new aws.ec2.SecurityGroup("cache-sg", {
        vpcId: input.vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`ec-allow-${key}`, {
            securityGroupId: cacheSg.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    const cluster = new aws.elasticache.ReplicationGroup("cache-cluster", {
        // "redis" is optional here and also the only allowed value, but we
        // set it here anyway to be explicit.
        engine: "redis",
        engineVersion: REDIS_VERSION,
        replicationGroupDescription: "redis-based elastic cache",
        nodeType: NODE_TYPE,
        securityGroupIds: [cacheSg.id],
        subnetGroupName: subnetGroup.name,
        transitEncryptionEnabled: true,
        atRestEncryptionEnabled: true,
        clusterMode: {
            numNodeGroups: 2,
            replicasPerNodeGroup: 1,
        },
        automaticFailoverEnabled: true,
        tags: { ...fennelStdTags },
    }, { provider })

    const primaryAddress = cluster.primaryEndpointAddress
    const replicaAddress = cluster.readerEndpointAddress

    const output: outputType = {
        primaryAddress,
        replicaAddress,
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
