import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi"
import * as fs from "fs";

const config = new pulumi.Config();

// TODO: Do not hardcode subnet, and use a private subnet.
const subnet = config.require("subnet")  // "subnet-0ac6f13fe8fcd49ef";

try {
    var startupScript = fs.readFileSync('startup-script.sh', 'utf8')
} catch (err) {
    console.error(err)
    process.exit()
}

// TODO: Associate with a security group that has access to EKS and other services.
const relay = new aws.ec2.Instance("tailscale-relay",
    {
        ami: config.require("ami"),
        instanceType: "t3.small",
        subnetId: subnet,
        associatePublicIpAddress: true,
        userData: startupScript,
        tags: {
            Name: "tailscale-relay",
        },
    }, { replaceOnChanges: ['*'], deleteBeforeReplace: true }
)
