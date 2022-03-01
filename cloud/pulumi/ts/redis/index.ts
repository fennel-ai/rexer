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
    connectedSecurityGroups: { [key: string]: string }
}

export type outputType = {
    clusterId: pulumi.Output<string>,
    clusterEndPoints: pulumi.Output<string[]>,
    clusterSecurityGroupIds: pulumi.Output<string[] | undefined>,
}

const REDIS_VERSION = "6.2";
const NODE_TYPE = "db.t4g.small";
// TODO: Increase replica count once we add more than one subnet to group.
const NUM_REPLICAS = 0;

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        vpcId: config.require(nameof<inputType>("vpcId")),
        azs: config.requireObject(nameof<inputType>("azs")),
        connectedSecurityGroups: config.requireObject(nameof<inputType>("connectedSecurityGroups")),
    }
}

export const setup = async (input: inputType) => {
    const provider = new aws.Provider("redis-aws-provider", {
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

    const subnetGroup = new aws.memorydb.SubnetGroup("redis-subnet-group", {
        subnetIds: subnetIds.ids,
        tags: { ...fennelStdTags },
    }, { provider })

    const redisSg = new aws.ec2.SecurityGroup("redis-sg", {
        namePrefix: "redis-sg-",
        vpcId: input.vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`allow-${key}`, {
            securityGroupId: redisSg.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    const cluster = pulumi.all(sgRules).apply(() => {
        return new aws.memorydb.Cluster("redis-db",
            {
                subnetGroupName: subnetGroup.id,
                aclName: "open-access",
                engineVersion: REDIS_VERSION,
                nodeType: NODE_TYPE,
                autoMinorVersionUpgrade: true,
                tlsEnabled: true,
                numReplicasPerShard: NUM_REPLICAS,
                securityGroupIds: [redisSg.id],
                tags: { ...fennelStdTags },
            }, { provider }
        )
    })

    const output: outputType = {
        clusterId: cluster.id,
        clusterEndPoints: cluster.clusterEndpoints.apply(endpoints => endpoints.map(endpoint => `${endpoint.address}:${endpoint.port}`)),
        clusterSecurityGroupIds: cluster.securityGroupIds,
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
