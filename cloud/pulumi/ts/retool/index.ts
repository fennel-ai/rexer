import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi"
import * as fs from "fs";

// import { fennelStdTags } from "../lib/util";

const config = new pulumi.Config();

try {
    var startupScript = fs.readFileSync('startup-script.sh', 'utf8')
} catch (err) {
    console.error(err)
    process.exit()
}

const provider = new aws.Provider("redis-aws-provider", {
    region: <aws.Region>config.require("region"),
    assumeRole: {
        roleArn: config.require("role"),
        // TODO: Also populate the externalId field to prevent "confused deputy"
        // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
    }
})

const retoolsg = new aws.ec2.SecurityGroup("retoolsg", {
    namePrefix: "retool-bastion-sg",
    vpcId: config.require("vpcId"),
    // tags: { ...fennelStdTags }
}, { provider })

const allowRetool = new aws.ec2.SecurityGroupRule("allow-retool", {
    securityGroupId: retoolsg.id,
    type: "ingress",
    protocol: "tcp",
    fromPort: 22,
    toPort: 22,
    cidrBlocks: [
        "52.175.251.223/32",
    ],
}, { provider })

const allowBastionToInternet = new aws.ec2.SecurityGroupRule("allow-bastion", {
    securityGroupId: retoolsg.id,
    type: "egress",
    protocol: "tcp",
    fromPort: 0,
    toPort: 65535,
    cidrBlocks: [
        "0.0.0.0/0",
    ],
}, { provider })

const relay = new aws.ec2.Instance("retool-bastion",
    {
        ami: config.require("ami"),
        instanceType: "t3.small",
        subnetId: config.require("subnet"),
        associatePublicIpAddress: true,
        userData: startupScript,
        userDataReplaceOnChange: true,
        vpcSecurityGroupIds: [retoolsg.id],
        tags: {
            "Name": "retool-bastion",
            // ...fennelStdTags
        },
    }, { provider })

export const retoolSg = retoolsg.id
export const retoolBastionPublicIp = relay.publicDns

// TODO: Create load-balancer that points to this bastion host.
