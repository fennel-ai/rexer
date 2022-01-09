import * as aws from "@pulumi/aws";
import * as fs from "fs";

const eip = new aws.ec2.Eip("tailscale-eip");

// TODO: Do not hardcode subnet, and use a private subnet.
const subnet = "subnet-0ac6f13fe8fcd49ef";

// Create NAT gateway which needed for internet connectivity from private subnets.
const ng = new aws.ec2.NatGateway("ng", {
    allocationId: eip.allocationId,
    subnetId: subnet,
})

export const gatewayid = ng.id

try {
    var startupScript = fs.readFileSync('startup-script.sh', 'utf8')
} catch (err) {
    console.error(err)
    process.exit()
}

// TODO: Associate with a security group that has access to EKS and other services.
const relay = new aws.ec2.Instance("tailscale-relay",
    {
        ami: "ami-052cef05d01020f1d",
        instanceType: "t3.small",
        subnetId: subnet,
        associatePublicIpAddress: true,
        userData: startupScript,
    }
)
