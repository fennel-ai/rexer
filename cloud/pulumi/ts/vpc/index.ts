import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws"
import * as process from "process";
import * as netmask from "netmask";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "aws": "v4.37.4"
}

export type inputType = {
    cidr: string
    region: string
    roleArn: string
}

export type outputType = {
    vpcId: pulumi.Output<string>
    publicSubnets: pulumi.Output<string>[]
    privateSubnets: pulumi.Output<string>[]
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        cidr: config.require(nameof<inputType>("cidr")),
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
    }
}

function createPrivateRouteTable(vpc: aws.ec2.Vpc): pulumi.Output<string> {
    const routeTable = new aws.ec2.RouteTable("private-rt", {
        vpcId: vpc.id,
    })
    return routeTable.id
}

function createPublicRouteTable(vpc: aws.ec2.Vpc): pulumi.Output<string> {
    const routeTable = new aws.ec2.RouteTable("public-rt", {
        vpcId: vpc.id,
    })
    return routeTable.id
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

    const vpc = new aws.ec2.Vpc("vpc", {
        cidrBlock: input.cidr,
        tags: {
            "Name": "fennel-vpc",
            ...fennelStdTags
        }
    }, { provider })

    // Divide the vpc into 4 subnets: 2 private and 2 public.
    const azs = await aws.getAvailabilityZones({}, { provider })

    console.log("Availability zones ", azs.names)
    const primaryAz = azs.names[0];
    const secondaryAz = azs.names[1];

    const [ip, mask] = input.cidr.split('/')
    const subnetMask = Number(mask) + 2

    let subnet = new netmask.Netmask(`${ip}/${subnetMask}`)
    const primaryPublicSubnet = new aws.ec2.Subnet("primary-public-subnet", {
        vpcId: vpc.id,
        cidrBlock: subnet.toString(),
        availabilityZone: primaryAz,
        tags: {
            "Name": "fennel-primary-public-subnet",
            ...fennelStdTags,
        }
    }, { provider })

    subnet = subnet.next()
    const secondaryPublicSubnet = new aws.ec2.Subnet("secondary-public-subnet", {
        vpcId: vpc.id,
        cidrBlock: subnet.toString(),
        availabilityZone: secondaryAz,
        tags: {
            "Name": "fennel-secondary-public-subnet",
            ...fennelStdTags,
        }
    }, { provider })

    subnet = subnet.next()
    const primaryPrivateSubnet = new aws.ec2.Subnet("primary-private-subnet", {
        vpcId: vpc.id,
        cidrBlock: subnet.toString(),
        availabilityZone: primaryAz,
        tags: {
            "Name": "fennel-primary-private-subnet",
            ...fennelStdTags,
        }
    }, { provider })

    subnet = subnet.next()
    const secondaryPrivateSubnet = new aws.ec2.Subnet("secondary-private-subnet", {
        vpcId: vpc.id,
        cidrBlock: subnet.toString(),
        availabilityZone: secondaryAz,
        tags: {
            "Name": "fennel-secondary-private-subnet",
            ...fennelStdTags,
        }
    }, { provider })

    const output: outputType = {
        vpcId: vpc.id,
        publicSubnets: [primaryPublicSubnet.id, secondaryPublicSubnet.id],
        privateSubnets: [primaryPrivateSubnet.id, secondaryPrivateSubnet.id],
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