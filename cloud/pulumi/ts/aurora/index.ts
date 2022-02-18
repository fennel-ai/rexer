import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as process from "process";
import * as passwordgen from "generate-password";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {}

export type inputType = {
    vpcId: string,
    eksSecurityGroup: string,
    minCapacity: number,
    maxCapacity: number,
}

export type outputType = {
    host: pulumi.Output<string>,
    username: string,
    password: string,
}


const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        vpcId: config.require(nameof<inputType>("vpcId")),
        minCapacity: config.requireNumber(nameof<inputType>("minCapacity")),
        maxCapacity: config.requireNumber(nameof<inputType>("maxCapacity")),
        eksSecurityGroup: config.require(nameof<inputType>("eksSecurityGroup")),
    }
}

export const setup = async (input: inputType) => {

    const subnetIds = await aws.ec2.getSubnetIds({
        vpcId: input.vpcId,
        // TODO: use better method for filtering private subnets.
        filters: [{
            name: "tag:Name",
            values: ["fennel-primary-private-subnet", "fennel-secondary-private-subnet"],
        }]
    })

    const subnetGroup = new aws.rds.SubnetGroup("db-subnetgroup", {
        subnetIds: subnetIds.ids,
        description: "Subnet group for primary database",
        tags: { ...fennelStdTags },
    })

    const securityGroup = new aws.ec2.SecurityGroup("db-sg", {
        namePrefix: "fenneldb-sg-",
        vpcId: input.vpcId,
        tags: { ...fennelStdTags },
    })

    const allowEksTraffic = new aws.ec2.SecurityGroupRule("allow-eks", {
        securityGroupId: securityGroup.id,
        sourceSecurityGroupId: input.eksSecurityGroup,
        // TODO: restrict port.
        fromPort: 0,
        toPort: 65535,
        type: "ingress",
        protocol: "tcp",
    })

    const masterUsername = "admin";
    const masterPassword = passwordgen.generate({ strict: true });

    const cluster = new aws.rds.Cluster("db-instance", {
        dbSubnetGroupName: subnetGroup.name,
        vpcSecurityGroupIds: [securityGroup.id],
        clusterIdentifierPrefix: "fenneldb-",
        engine: aws.rds.EngineType.AuroraMysql,
        engineMode: aws.rds.EngineMode.Serverless,
        engineVersion: "5.7.mysql_aurora.2.10.2",
        masterUsername: masterUsername,
        masterPassword: masterPassword,
        scalingConfiguration: {
            minCapacity: input.minCapacity,
            maxCapacity: input.maxCapacity,
        },
        tags: { ...fennelStdTags }
    })

    const output: outputType = {
        host: cluster.endpoint,
        username: masterUsername,
        password: masterPassword,
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