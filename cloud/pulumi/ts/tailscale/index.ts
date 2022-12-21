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

// TODO(mohit): Consider automating setting authkey for tailscale using https://www.pulumi.com/registry/packages/tailscale/api-docs/tailnetkey/
//
// Currently the key could expire and this machine will fail to setup tailscale.

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
const relay = new aws.ec2.Instance("tailscale-relay",
    {
        ami: config.require("ami"),
        instanceType: "t3.small",
        subnetId: subnet,
        associatePublicIpAddress: true,
        userData: startupScript,
        keyName: keypair.keyName,
        tags: {
            Name: "tailscale-relay",
        },
    }, { replaceOnChanges: ['*'], deleteBeforeReplace: true }
)
