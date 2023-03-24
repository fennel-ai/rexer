import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as path from "path";


import { fennelStdTags } from "../lib/util";

export const plugins = {
    "aws": "v5.0.0"
}

export type inputType = {
    planeId: number,
    planeName?: string,

    region: string,
    roleArn: pulumi.Input<string>,
    protect: boolean,

    // private subnets associated in this VPC
    privateSubnets: pulumi.Output<string[]>,

    brokerType: string,
    numberOfBrokerNodes: number,
    storageVolumeSizeGiB: number,

    // vpc id
    vpcId: pulumi.Output<string>,

    // allow connectivity from EKS and control plane
    connectedSecurityGroups: Record<string, pulumi.Output<string>>,
    connectedCidrBlocks?: string[],
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    clusterName: string,
    clusterArn: string,
    clusterSgId: string,
    mskUsername: string,
    mskPassword: string,
    zookeeperConnectString: string,
    // comma separated bootstrap servers in and across multiple AZs
    bootstrapBrokers: string,
    // comma separated bootstrap servers in and across multiple AZs, which is to be used with IAM authentication
    bootstrapBrokersIam: string,
    numBrokers: number,

    // plugin
    s3ConnectPluginArn: string,
    s3ConnectPluginRevision: number,

    // worker
    s3ConnectWorkerArn: string,
    s3ConnectWorkerRev: number,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const awsProvider = new aws.Provider("msk-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // create cloudwatch log group
    const logGroup = new aws.cloudwatch.LogGroup("msk-log-group", {
        name: `/aws/msk/p-${input.planeId}-brokers`,
        // retain logs for 5 days
        retentionInDays: 5,
    }, { provider: awsProvider });

    // validate if the number of broker nodes is a multiple of the configured AZs
    input.privateSubnets.apply(subnets => {
        if (input.numberOfBrokerNodes % subnets.length !== 0) {
            console.log('Number of broker nodes to be configured should be a multiple of subnets configured. ',
                'Given number of nodes: ', input.numberOfBrokerNodes, ' configured subnets: ', subnets.length);
            process.exit(1);
        }
    });

    // create security group and configure security group rules to allow ingress from EKS and control-plane
    //
    // control-plane will allow running kafka commands from local machines and from the control plane (this might help
    // debug and with developer velocity).
    const sg = new aws.ec2.SecurityGroup("msk-sg", {
        vpcId: input.vpcId,
        namePrefix: `p-${input.planeId}-msk-sg-`,
        tags: { ...fennelStdTags },
    }, { provider: awsProvider });
    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-msk-allow-${key}`, {
            securityGroupId: sg.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider: awsProvider }).id);
    }
    if (input.connectedCidrBlocks !== undefined) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-msk-allow-connected-cidr`, {
            securityGroupId: sg.id,
            cidrBlocks: input.connectedCidrBlocks,
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider: awsProvider }).id);
    }

    // allow self inbound traffic - this is required for MSK connect
    sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-msk-allow-selftraffic`, {
        securityGroupId: sg.id,
        self: true,
        fromPort: 0,
        toPort: 65535,
        type: "ingress",
        protocol: "all",
    }, { provider: awsProvider }).id);

    // allow self outbound traffic - this is required for MSK connect
    sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-msk-allow-outbound-selftraffic`, {
        securityGroupId: sg.id,
        self: true,
        fromPort: 0,
        toPort: 65535,
        type: "egress",
        protocol: "all",
    }, { provider: awsProvider }).id);

    // allow traffic to the internet
    sgRules.push(new aws.ec2.SecurityGroupRule(`p-${input.planeId}-msk-allow-outbound-internet`, {
        securityGroupId: sg.id,
        cidrBlocks: ["0.0.0.0/0"],
        fromPort: 0,
        toPort: 65535,
        type: "egress",
        protocol: "all",
    }, { provider: awsProvider }).id);

    // setup kafka broker configuration
    const config = new aws.msk.Configuration("msk-cluster-config", {
        // This is required to assign the closest (in the same AZ) broker to the consumer
        //
        // `broker.rack` is set by the msk cluster, see - https://aws.amazon.com/blogs/big-data/reduce-network-traffic-costs-of-your-amazon-msk-consumers-with-rack-awareness/
        serverProperties: `replica.selector.class = org.apache.kafka.common.replica.RackAwareReplicaSelector`,
        description: `plane ${input.planeId} kafka broker configuration`,

        // this can be a list of kafka versions for which the provided server properties are valid for
        kafkaVersions: ["2.6.2"],
        name: `p-${input.planeId}-cluster-config`
    }, { provider: awsProvider });

    const config2 = new aws.msk.Configuration("msk-cluster-config-2", {
        // This is required to assign the closest (in the same AZ) broker to the consumer
        //
        // `broker.rack` is set by the msk cluster, see - https://aws.amazon.com/blogs/big-data/reduce-network-traffic-costs-of-your-amazon-msk-consumers-with-rack-awareness/
        serverProperties: `replica.selector.class = org.apache.kafka.common.replica.RackAwareReplicaSelector`,
        description: `plane ${input.planeId} kafka broker configuration`,

        // this can be a list of kafka versions for which the provided server properties are valid for
        kafkaVersions: ["2.6.2", "2.7.0"],
        name: `p-${input.planeId}-cluster-config-2`
    }, { provider: awsProvider });

    const config3 = new aws.msk.Configuration("msk-cluster-config-3", {
        // This is required to assign the closest (in the same AZ) broker to the consumer
        //
        // `broker.rack` is set by the msk cluster, see - https://aws.amazon.com/blogs/big-data/reduce-network-traffic-costs-of-your-amazon-msk-consumers-with-rack-awareness/
        serverProperties: `
            replica.selector.class = org.apache.kafka.common.replica.RackAwareReplicaSelector
            
            auto.create.topics.enable = true
            
            delete.topic.enable=true
        `,
        description: `plane ${input.planeId} kafka broker configuration for kafka 3.0`,

        // this can be a list of kafka versions for which the provided server properties are valid for
        kafkaVersions: ["3.1.1", "3.2.0"],
        name: `p-${input.planeId}-cluster-config-3`
    }, { provider: awsProvider });

    // setup msk cluster
    const cluster = new aws.msk.Cluster("msk-cluster", {
        // this is needed for consumer group offset syncs to work for Mirror Maker 2.0 which allows for migration
        // from Confluent to MSK and in the future possibility to back up our MSK cluster for disaster recovery
        //
        // https://cwiki.apache.org/confluence/display/KAFKA/KIP-545:+support+automated+consumer+offset+sync+across+clusters+in+MM+2.0
        kafkaVersion: "3.2.0",
        clusterName: `p-${input.planeId}-kafka-cluster`,
        numberOfBrokerNodes: input.numberOfBrokerNodes,
        brokerNodeGroupInfo: {
            instanceType: input.brokerType,
            clientSubnets: input.privateSubnets,
            ebsVolumeSize: input.storageVolumeSizeGiB,
            securityGroups: [sg.id],
        },
        clientAuthentication: {
            sasl: {
                scram: true,
                // enable IAM client side authentication to allow MSK connector to be able to read the cluster
                iam: true
            },
        },

        // broker configuration
        configurationInfo: {
            arn: config3.arn,
            revision: config3.latestRevision,
        },

        // TODO(mohit): See if encryption should be enabled here

        // enabled prometheus scrapable metrics. also enable cloudwatch metrics for initial monitoring
        openMonitoring: {
            // TODO(mohit): configure the prometheus instance running the plane to be able to scrape these metrics
            // see - https://docs.aws.amazon.com/msk/latest/developerguide/open-monitoring.html
            prometheus: {
                jmxExporter: {
                    enabledInBroker: true,
                },
                nodeExporter: {
                    enabledInBroker: true,
                }
            },
        },
        // enable only default level metrics, since for the rest of the levels, we need to pay at cloudwatch rates
        enhancedMonitoring: "DEFAULT",
        loggingInfo: {
            brokerLogs: {
                cloudwatchLogs: {
                    enabled: true,
                    logGroup: logGroup.name,
                }
            }
        }
    }, { provider: awsProvider });

    // create AWS secret and associate it with the MSK cluster
    const kmsKey = new aws.kms.Key("msk-key", {
        description: "",
    }, { provider: awsProvider });
    const secret = new aws.secretsmanager.Secret("msk-secret", {
        kmsKeyId: kmsKey.keyId,
        // MSK secrets must start with AmazonMSK_:
        // https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html#msk-password-limitations
        namePrefix: `AmazonMSK_p-${input.planeId}-msk-secret-`,
    }, { provider: awsProvider });
    const mskUserName = `p-${input.planeId}-username`;
    const mskPassword = `p-${input.planeId}-password`;
    const secretVersion = new aws.secretsmanager.SecretVersion("msk-secret-version", {
        secretId: secret.id,
        secretString: JSON.stringify({
            username: mskUserName,
            password: mskPassword,
        }),
    }, { provider: awsProvider });
    const mskSecretAssociation = new aws.msk.ScramSecretAssociation("msk-secret-association", {
        clusterArn: cluster.arn,
        secretArnLists: [secret.arn],
    }, { provider: awsProvider, dependsOn: [secretVersion] });
    const secretPolicy = secret.arn.apply(secretArn => {
        return new aws.secretsmanager.SecretPolicy("msk-secret-policy", {
            secretArn: secret.arn,
            policy: JSON.stringify({
                Version: "2012-10-17",
                Statement: [{
                    Effect: "Allow",
                    Principal: {
                        Service: "kafka.amazonaws.com",
                    },
                    Action: "secretsmanager:getSecretValue",
                    Resource: `${secretArn}`
                }],
            })
        }, { provider: awsProvider });
    });

    // create the s3 connector plugin which will be used by every tier level MSK connector to run
    let bucketName: string;
    if (input.planeName) {
        bucketName = `p-${input.planeName}-mskconnect-plugins`;
    } else {
        bucketName = `p-${input.planeId}-mskconnect-plugins`;
    }
    const pluginBucket = new aws.s3.BucketV2("mskconnect-plugin-bucket", {
        bucket: bucketName,
        forceDestroy: true,
    }, { provider: awsProvider, protect: input.protect });
    const root = process.env.FENNEL_ROOT!;
    const pluginPath = path.join(root, "/cloud/mskconnect/plugins2.zip")
    const pluginObject = new aws.s3.BucketObjectv2("s3connect-zip", {
        bucket: pluginBucket.id,
        key: "s3connect2.zip",
        source: new pulumi.asset.FileAsset(pluginPath),
    }, { provider: awsProvider });
    const customPlugin = new aws.mskconnect.CustomPlugin("s3connect-plugin", {
        contentType: "ZIP",
        location: {
            s3: {
                bucketArn: pluginBucket.arn,
                fileKey: pluginObject.key,
            },
        },
    }, { provider: awsProvider });
    const workerConf = new aws.mskconnect.WorkerConfiguration("msk-workerconf", {
        propertiesFileContent: `key.converter=org.apache.kafka.connect.storage.StringConverter
        value.converter=org.apache.kafka.connect.json.JsonConverter
        value.converter.schemas.enable=false`,
    }, { provider: awsProvider });

    return pulumi.output({
        clusterName: cluster.clusterName,
        clusterArn: cluster.arn,
        clusterSgId: sg.id,
        mskUsername: mskUserName,
        mskPassword: mskPassword,
        zookeeperConnectString: cluster.zookeeperConnectString,
        bootstrapBrokers: cluster.bootstrapBrokersSaslScram,
        bootstrapBrokersIam: cluster.bootstrapBrokersSaslIam,
        numBrokers: input.numberOfBrokerNodes,

        s3ConnectPluginArn: customPlugin.arn,
        s3ConnectPluginRevision: customPlugin.latestRevision,

        s3ConnectWorkerArn: workerConf.arn,
        s3ConnectWorkerRev: workerConf.latestRevision,
    });
}
