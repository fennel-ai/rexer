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
    privateRouteTable: pulumi.Output<string>
    publicRouteTable: pulumi.Output<string>
    publicNacl: pulumi.Output<string>
    privateNacl: pulumi.Output<string>
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        cidr: config.require(nameof<inputType>("cidr")),
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
    }
}

// TODO: Tighten rules for more security.
function createPublicNacl(vpc: aws.ec2.Vpc, subnets: pulumi.Output<string>[], provider: aws.Provider): pulumi.Output<string> {
    const privateNacl = new aws.ec2.NetworkAcl("public-nacl", {
        vpcId: vpc.id,
        subnetIds: subnets,
        egress: [
            // Allow all egress TCP traffic.
            {
                ruleNo: 100,
                action: "ALLOW",
                cidrBlock: "0.0.0.0/0",
                fromPort: 0,
                toPort: 65535,
                protocol: "tcp",
            },
        ],
        ingress: [
            // Allow all ingress TCP traffic.
            {
                ruleNo: 100,
                action: "ALLOW",
                cidrBlock: "0.0.0.0/0",
                fromPort: 0,
                toPort: 65535,
                protocol: "tcp",
            },
        ],
        tags: { ...fennelStdTags }
    }, { provider })

    return privateNacl.id
}

// TODO: Tighten rules for more security.
function createPrivateNacl(vpc: aws.ec2.Vpc, subnets: pulumi.Output<string>[], provider: aws.Provider): pulumi.Output<string> {
    const privateNacl = new aws.ec2.NetworkAcl("private-nacl", {
        vpcId: vpc.id,
        subnetIds: subnets,
        egress: [
            // Allow all egress TCP traffic.
            {
                ruleNo: 100,
                action: "ALLOW",
                cidrBlock: "0.0.0.0/0",
                fromPort: 0,
                toPort: 65535,
                protocol: "tcp",
            },
            // Allow all traffic within vpc.
            {
                ruleNo: 101,
                action: "ALLOW",
                cidrBlock: vpc.cidrBlock,
                fromPort: 0,
                toPort: 0,
                protocol: "-1",
            },
        ],
        ingress: [
            // Allow all ingress TCP traffic.
            {
                ruleNo: 100,
                action: "ALLOW",
                cidrBlock: "0.0.0.0/0",
                fromPort: 0,
                toPort: 65535,
                protocol: "tcp",
            },
            // Allow all traffic within vpc.
            {
                ruleNo: 101,
                action: "ALLOW",
                cidrBlock: vpc.cidrBlock,
                fromPort: 0,
                toPort: 0,
                protocol: "-1",
            },
        ],
        tags: { ...fennelStdTags }
    }, { provider })

    return privateNacl.id
}

function createPrivateSubnet(name: string, vpcId: pulumi.Output<string>, subnet: string, az: string, provider: aws.Provider) {
    return new aws.ec2.Subnet(name, {
        vpcId: vpcId,
        cidrBlock: subnet,
        availabilityZone: az,
        tags: {
            "Name": name,
            "kubernetes.io/role/internal-elb": "1",
            ...fennelStdTags,
        }
    }, { provider })
}

function createPublicSubnet(name: string, vpcId: pulumi.Output<string>, subnet: string, az: string, provider: aws.Provider) {
    return new aws.ec2.Subnet(name, {
        vpcId: vpcId,
        cidrBlock: subnet,
        availabilityZone: az,
        tags: {
            "Name": name,
            "kubernetes.io/role/elb": "1",
            ...fennelStdTags,
        }
    }, { provider })
}

function setupPrivateRouteTable(vpcId: pulumi.Output<string>, subnets: pulumi.Output<string>[], publicSubnet: pulumi.Output<string>, provider: aws.Provider): pulumi.Output<string> {
    const eip = new aws.ec2.Eip("eip", {
        tags: { ...fennelStdTags }
    }, { provider })

    const natGateway = new aws.ec2.NatGateway("nat-gateway", {
        allocationId: eip.allocationId,
        subnetId: publicSubnet,
        tags: { ...fennelStdTags }
    }, { provider })

    const privateRt = new aws.ec2.RouteTable("private-rt", {
        vpcId: vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    // Create routes outside route-table so we can add routes in other projects as well.
    const allowAll = new aws.ec2.Route("allow-all-private-rt", {
        routeTableId: privateRt.id,
        destinationCidrBlock: "0.0.0.0/0",
        natGatewayId: natGateway.id,
    }, { provider })

    subnets.map((subnetId, idx) => {
        return new aws.ec2.RouteTableAssociation(`rt-assoc-private-${idx}`, {
            subnetId: subnetId,
            routeTableId: privateRt.id,
        }, { provider })
    })

    return privateRt.id
}

function setupPublicRouteTable(vpcId: pulumi.Output<string>, subnets: pulumi.Output<string>[], provider: aws.Provider): pulumi.Output<string> {
    const igw = new aws.ec2.InternetGateway("internet-gateway", {
        vpcId: vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    const publicRt = new aws.ec2.RouteTable("public-rt", {
        vpcId: vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    // Create routes outside route-table so we can add routes in other projects as well.
    const allowAll = new aws.ec2.Route("allow-all-public-rt", {
        routeTableId: publicRt.id,
        destinationCidrBlock: "0.0.0.0/0",
        gatewayId: igw.id,
    }, { provider })

    subnets.map((subnetId, idx) => {
        return new aws.ec2.RouteTableAssociation(`rt-assoc-public-${idx}`, {
            subnetId: subnetId,
            routeTableId: publicRt.id,
        }, { provider })
    })

    return publicRt.id
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

    const vpcCidr = input.cidr

    const vpc = new aws.ec2.Vpc("vpc", {
        cidrBlock: vpcCidr,
        tags: {
            "Name": "fennel-vpc",
            ...fennelStdTags
        }
    }, { provider })

    const vpcId = vpc.id;

    // Divide the vpc into 4 subnets: 2 private and 2 public.
    const azs = await aws.getAvailabilityZones({}, { provider })

    console.log("Availability zones ", azs.names)
    const primaryAz = azs.names[0];
    const secondaryAz = azs.names[1];

    const [ip, mask] = input.cidr.split('/')
    const subnetMask = Number(mask) + 2

    let subnet = new netmask.Netmask(`${ip}/${subnetMask}`)
    const primaryPublicSubnet = createPublicSubnet("fennel-primary-public-subnet", vpcId, subnet.toString(), primaryAz, provider)

    subnet = subnet.next()
    const secondaryPublicSubnet = createPublicSubnet("fennel-secondary-public-subnet", vpcId, subnet.toString(), secondaryAz, provider)

    subnet = subnet.next()
    const primaryPrivateSubnet = createPrivateSubnet("fennel-primary-private-subnet", vpcId, subnet.toString(), primaryAz, provider)

    subnet = subnet.next()
    const secondaryPrivateSubnet = createPrivateSubnet("fennel-secondary-private-subnet", vpcId, subnet.toString(), secondaryAz, provider)

    const privateSubnets = [primaryPrivateSubnet.id, secondaryPrivateSubnet.id];
    const publicSubnets = [primaryPublicSubnet.id, secondaryPublicSubnet.id];

    const privateNacl = createPrivateNacl(vpc, privateSubnets, provider)
    const publicNacl = createPublicNacl(vpc, publicSubnets, provider)

    const publicRouteTable = setupPublicRouteTable(vpcId, publicSubnets, provider)
    const privateRouteTable = setupPrivateRouteTable(vpcId, privateSubnets, primaryPublicSubnet.id, provider)

    const output: outputType = {
        vpcId,
        publicSubnets,
        privateSubnets,
        privateNacl,
        publicNacl,
        privateRouteTable,
        publicRouteTable,
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