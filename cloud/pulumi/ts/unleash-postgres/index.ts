import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import {fennelStdTags} from "../lib/util";
import {UNLEASH_PASSWORD, UNLEASH_USERNAME} from "../tier-consts/consts";

export const plugins = {
    "aws": "v4.38.1",
}

export type inputType = {
    roleArn: string,
    region: string,
    vpcId: pulumi.Output<string>,
    minCapacity: number,
    maxCapacity: number,
    connectedSecurityGroups: Record<string, pulumi.Output<string>>,
    connectedCidrBlocks?: string[],
    planeId: number,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    host: string,
    port: number,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider(`p-${input.planeId}-unleash-aurora-aws-provider`, {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const subnetIds = input.vpcId.apply(async vpcId => {
        return await aws.ec2.getSubnetIds({
            vpcId: vpcId,
            // TODO: use better method for filtering private subnets.
            filters: [{
                name: "tag:Name",
                values: [`p-${input.planeId}-primary-private-subnet`, `p-${input.planeId}-secondary-private-subnet`],
            }]
        }, { provider });
    });

    const subnetGroup = new aws.rds.SubnetGroup(`p-${input.planeId}-unleash-db-subnetgroup`, {
        subnetIds: subnetIds.ids,
        description: "Subnet group for unleash postgres database",
        tags: { ...fennelStdTags },
    }, { provider });

    const securityGroup = new aws.ec2.SecurityGroup(`p-${input.planeId}-unleash-db-sg`, {
        namePrefix: `p-${input.planeId}-unleash-db-sg-`,
        vpcId: input.vpcId,
        tags: { ...fennelStdTags },
    }, { provider });

    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-unleash-allow-${key}`, {
            securityGroupId: securityGroup.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }
    if (input.connectedCidrBlocks !== undefined) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-unleash-aurora-allow-connected-cidr`, {
            securityGroupId: securityGroup.id,
            cidrBlocks: input.connectedCidrBlocks,
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    const cluster = new aws.rds.Cluster(`p-${input.planeId}-unleash-db-instance`, {
        // Apply any changes proposed immediately instead of applying them during maintenance window
        applyImmediately: true,
        dbSubnetGroupName: subnetGroup.name,
        vpcSecurityGroupIds: [securityGroup.id],
        clusterIdentifierPrefix: `p-${input.planeId}-unleash-db-`,
        engine: aws.rds.EngineType.AuroraPostgresql,
        engineMode: aws.rds.EngineMode.Serverless,
        masterUsername: UNLEASH_USERNAME,
        masterPassword: UNLEASH_PASSWORD,
        scalingConfiguration: {
            minCapacity: input.minCapacity,
            maxCapacity: input.maxCapacity,
        },
        skipFinalSnapshot: true,
        tags: { ...fennelStdTags }
    }, { provider, protect: input.protect });

    const output = pulumi.output({
        host: cluster.endpoint,
        port: cluster.port,
    });
    return output
}
