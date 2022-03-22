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
    "aws": "v4.38.0"
}

export type controlPlaneConfig = {
    roleArn: string,
    region: string,
    accountId: string,
    vpcId: string,
    cidrBlock: string,
    routeTableId: string,
}

export type inputType = {
    cidr: string
    region: string
    roleArn: string
    controlPlane: controlPlaneConfig,
    planeId: number,
}

export type outputType = {
    vpcId: string,
    publicSubnets: string[],
    privateSubnets: string[],
    privateRouteTable: string,
    publicRouteTable: string,
    publicNacl: string,
    privateNacl: string,
    azs: string[]
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        cidr: config.require(nameof<inputType>("cidr")),
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        controlPlane: config.requireObject(nameof<inputType>("controlPlane")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),
    }
}

// TODO: Tighten rules for more security.
function createPublicNacl(input: inputType, vpc: aws.ec2.Vpc, subnets: pulumi.Output<string>[], provider: aws.Provider): pulumi.Output<string> {
    const privateNacl = new aws.ec2.NetworkAcl(`p-${input.planeId}-public-nacl`, {
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
function createPrivateNacl(input: inputType, vpc: aws.ec2.Vpc, subnets: pulumi.Output<string>[], provider: aws.Provider): pulumi.Output<string> {
    const privateNacl = new aws.ec2.NetworkAcl(`p-${input.planeId}-private-nacl`, {
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

function setupPrivateRouteTable(input: inputType, vpcId: pulumi.Output<string>, subnets: pulumi.Output<string>[], publicSubnet: pulumi.Output<string>, provider: aws.Provider): pulumi.Output<string> {
    const eip = new aws.ec2.Eip(`p-${input.planeId}-eip`, {
        tags: { ...fennelStdTags }
    }, { provider })

    const natGateway = new aws.ec2.NatGateway(`p-${input.planeId}-nat-gateway`, {
        allocationId: eip.allocationId,
        subnetId: publicSubnet,
        tags: { ...fennelStdTags }
    }, { provider })

    const privateRt = new aws.ec2.RouteTable(`p-${input.planeId}-private-rt`, {
        vpcId: vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    // Create routes outside route-table so we can add routes in other projects as well.
    const allowAll = new aws.ec2.Route(`p-${input.planeId}-allow-all-private-rt`, {
        routeTableId: privateRt.id,
        destinationCidrBlock: "0.0.0.0/0",
        natGatewayId: natGateway.id,
    }, { provider })

    subnets.map((subnetId, idx) => {
        return new aws.ec2.RouteTableAssociation(`p-${input.planeId}-rt-assoc-private-${idx}`, {
            subnetId: subnetId,
            routeTableId: privateRt.id,
        }, { provider })
    })

    return privateRt.id
}

function setupPublicRouteTable(input: inputType, vpcId: pulumi.Output<string>, subnets: pulumi.Output<string>[], provider: aws.Provider): pulumi.Output<string> {
    const igw = new aws.ec2.InternetGateway(`p-${input.planeId}-internet-gateway`, {
        vpcId: vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    const publicRt = new aws.ec2.RouteTable(`p-${input.planeId}-public-rt`, {
        vpcId: vpcId,
        tags: { ...fennelStdTags }
    }, { provider })

    // Create routes outside route-table so we can add routes in other projects as well.
    const allowAll = new aws.ec2.Route(`p-${input.planeId}-allow-all-public-rt`, {
        routeTableId: publicRt.id,
        destinationCidrBlock: "0.0.0.0/0",
        gatewayId: igw.id,
    }, { provider })

    subnets.map((subnetId, idx) => {
        return new aws.ec2.RouteTableAssociation(`p-${input.planeId}-rt-assoc-public-${idx}`, {
            subnetId: subnetId,
            routeTableId: publicRt.id,
        }, { provider })
    })

    return publicRt.id
}

function createVpcPeeringConnection(vpc: aws.ec2.Vpc, routeTables: pulumi.Output<string>[], input: inputType, provider: aws.Provider): aws.ec2.VpcPeeringConnection {
    // create peering connection between vpc and control-plane vpc.
    const peeringConnection = new aws.ec2.VpcPeeringConnection(`p-${input.planeId}-peering-connection`, {
        vpcId: vpc.id,
        peerVpcId: input.controlPlane.vpcId,
        peerOwnerId: input.controlPlane.accountId,
        peerRegion: input.controlPlane.region,
        tags: {
            ...fennelStdTags,
            Side: "Requester",
        }
    }, { provider })

    const controlPlaneProvider = new aws.Provider("vpc-control-plane-provider", {
        region: <aws.Region>input.controlPlane.region,
        assumeRole: {
            roleArn: input.controlPlane.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const peeringConnectionAcceptor = new aws.ec2.VpcPeeringConnectionAccepter(`p-${input.planeId}-peering-connection-acceptor`, {
        vpcPeeringConnectionId: peeringConnection.id,
        autoAccept: true,
        accepter: {
            allowRemoteVpcDnsResolution: true,
        },
        tags: {
            ...fennelStdTags,
            Side: "Acceptor",
        }
    }, { provider: controlPlaneProvider })

    const controlPlaneToDataPlane = new aws.ec2.Route(`p-${input.planeId}-route-to-data-plane`, {
        routeTableId: input.controlPlane.routeTableId,
        vpcPeeringConnectionId: peeringConnection.id,
        destinationCidrBlock: vpc.cidrBlock,
    }, { provider: controlPlaneProvider, deleteBeforeReplace: true })

    const routes = routeTables.map((rt, idx) => {
        return new aws.ec2.Route(`p-${input.planeId}-route-to-control-plane-${idx}`, {
            routeTableId: rt,
            vpcPeeringConnectionId: peeringConnection.id,
            destinationCidrBlock: input.controlPlane.cidrBlock,
        }, { provider })
    })

    return peeringConnection
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {

    const provider = new aws.Provider("aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const vpcCidr = input.cidr

    const vpc = new aws.ec2.Vpc(`p-${input.planeId}-vpc`, {
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
    const primaryPublicSubnet = createPublicSubnet(`p-${input.planeId}-primary-public-subnet`, vpcId, subnet.toString(), primaryAz, provider)

    subnet = subnet.next()
    const secondaryPublicSubnet = createPublicSubnet(`p-${input.planeId}-secondary-public-subnet`, vpcId, subnet.toString(), secondaryAz, provider)

    subnet = subnet.next()
    const primaryPrivateSubnet = createPrivateSubnet(`p-${input.planeId}-primary-private-subnet`, vpcId, subnet.toString(), primaryAz, provider)

    subnet = subnet.next()
    const secondaryPrivateSubnet = createPrivateSubnet(`p-${input.planeId}-secondary-private-subnet`, vpcId, subnet.toString(), secondaryAz, provider)

    const privateSubnets = [primaryPrivateSubnet.id, secondaryPrivateSubnet.id];
    const publicSubnets = [primaryPublicSubnet.id, secondaryPublicSubnet.id];

    const publicRouteTable = setupPublicRouteTable(input, vpcId, publicSubnets, provider)
    const privateRouteTable = setupPrivateRouteTable(input, vpcId, privateSubnets, primaryPublicSubnet.id, provider)

    const peeringConnection = createVpcPeeringConnection(vpc, [privateRouteTable], input, provider)

    const privateNacl = createPrivateNacl(input, vpc, privateSubnets, provider)
    const publicNacl = createPublicNacl(input, vpc, publicSubnets, provider)

    const output = pulumi.output({
        vpcId,
        publicSubnets,
        privateSubnets,
        privateNacl,
        publicNacl,
        privateRouteTable,
        publicRouteTable,
        azs: [primaryAz, secondaryAz],
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
