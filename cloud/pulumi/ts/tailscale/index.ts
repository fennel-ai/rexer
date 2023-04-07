import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi"

import * as fs from "fs";

const config = new pulumi.Config();

// Read the startup script template.
try {
    var template = fs.readFileSync('startup-script.sh', 'utf8');
} catch (err) {
    console.error(err)
    process.exit()
}

const tailscaleAuthkey = config.requireSecret("authKey")
var startupScript = tailscaleAuthkey.apply(key => template.replace("%TAILSCALE_AUTHKEY%", key))

try {
    var publickey: string = fs.readFileSync('keypair/id_rsa.pub', 'utf8');
} catch (err) {
    console.log("failed to read key-pair: " + err);
    process.exit();
}
const keypair = new aws.ec2.KeyPair("tailscale-keypair", {
    publicKey: publickey,
});

// TODO: Associate with a security group that has access to EKS and other services.
const subnet = config.require("subnet")
const relay = new aws.ec2.Instance("tailscale-relay",
    {
        ami: config.require("ami"),
        instanceType: "t3.small",
        subnetId: subnet,
        associatePublicIpAddress: false,
        userData: startupScript,
        keyName: keypair.keyName,
        tags: {
            Name: "tailscale-relay",
        },
    }, { replaceOnChanges: ['*'], deleteBeforeReplace: true }
)
