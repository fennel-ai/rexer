import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as process from "process";
import * as uuid from "uuid";
import { getPrefix, Scope } from "../lib/util";

import { nameof, fennelStdTags } from "../lib/util";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    roleArn: pulumi.Input<string>,
    region: string,
    vpcId: pulumi.Output<string>,
    minCapacity: number,
    maxCapacity: number,
    username: string,
    password: pulumi.Output<string>,
    connectedSecurityGroups: Record<string, pulumi.Output<string>>,
    connectedCidrBlocks?: string[],
    planeId: number,
    // Upon deletion of the cluster, should the final snapshot be skipped.
    // We should ideally set this to `false` for production tiers and true for the test/staging tiers.
    skipFinalSnapshot: boolean,
    protect: boolean,
    scope: Scope,
}

export type outputType = {
    host: string,
    user: string,
    password: string,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        roleArn: config.require(nameof<inputType>("roleArn")),
        region: config.require(nameof<inputType>("region")),
        vpcId: pulumi.output(config.require(nameof<inputType>("vpcId"))),
        minCapacity: config.requireNumber(nameof<inputType>("minCapacity")),
        maxCapacity: config.requireNumber(nameof<inputType>("maxCapacity")),
        connectedSecurityGroups: config.requireObject(nameof<inputType>("connectedSecurityGroups")),
        connectedCidrBlocks: config.getObject(nameof<inputType>("connectedCidrBlocks")),
        username: config.require(nameof<inputType>("username")),
        password: config.requireSecret(nameof<inputType>("password")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),
        skipFinalSnapshot: config.requireBoolean(nameof<inputType>("skipFinalSnapshot")),
        protect: config.requireBoolean(nameof<inputType>("protect")),
        scope: config.requireNumber(nameof<inputType>("scope")),
    }
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("aurora-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const subnetIds = input.vpcId.apply(async vpcId => {
        var prefix: string
        if (input.scope === Scope.MOTHERSHIP) {
            prefix = "fennel"
        } else {
            prefix = getPrefix(input.scope, input.planeId)
        }
        return await aws.ec2.getSubnetIds({
            vpcId: vpcId,
            // TODO: use better method for filtering private subnets.
            filters: [{
                name: "tag:Name",
                values: [`${prefix}-primary-private-subnet`, `${prefix}-secondary-private-subnet`],
            }]
        }, { provider })
    })

    const subnetGroup = new aws.rds.SubnetGroup(`${getPrefix(input.scope, input.planeId)}-db-subnetgroup`, {
        subnetIds: subnetIds.ids,
        description: "Subnet group for primary database",
        tags: { ...fennelStdTags },
    }, { provider })

    const securityGroup = new aws.ec2.SecurityGroup(`${getPrefix(input.scope, input.planeId)}-db-sg`, {
        namePrefix: `${getPrefix(input.scope, input.planeId)}-db-sg-`,
        vpcId: input.vpcId,
        tags: { ...fennelStdTags },
    }, { provider })

    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`${getPrefix(input.scope, input.planeId)}-allow-${key}`, {
            securityGroupId: securityGroup.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }
    if (input.connectedCidrBlocks !== undefined) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`${getPrefix(input.scope, input.planeId)}-aurora-allow-connected-cidr`, {
            securityGroupId: securityGroup.id,
            cidrBlocks: input.connectedCidrBlocks,
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    // Generate a random ID for the final snapshot identifier since it is possible that we destroy
    // an RDS cluster and bring up another one in the same plane.
    const snapshotId = uuid.v4();

    const cluster = new aws.rds.Cluster("db-instance", {
        // Apply any changes proposed immediately instead of applying them during maintenance window
        applyImmediately: true,
        dbSubnetGroupName: subnetGroup.name,
        vpcSecurityGroupIds: [securityGroup.id],
        clusterIdentifierPrefix: `${getPrefix(input.scope, input.planeId)}-db-`,
        engine: aws.rds.EngineType.AuroraMysql,
        engineMode: aws.rds.EngineMode.Serverless,
        engineVersion: "5.7.mysql_aurora.2.08.3",
        masterUsername: input.username,
        masterPassword: input.password,
        scalingConfiguration: {
            minCapacity: input.minCapacity,
            maxCapacity: input.maxCapacity,
        },
        skipFinalSnapshot: input.skipFinalSnapshot,
        finalSnapshotIdentifier: `${getPrefix(input.scope, input.planeId)}-${snapshotId}`,
        tags: { ...fennelStdTags }
    }, { provider, protect: input.protect })

    const output = pulumi.output({
        host: cluster.endpoint,
        user: input.username,
        password: input.password,
    })

    return output
}

async function run() {
    let output: pulumi.Output<outputType> | undefined;
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
