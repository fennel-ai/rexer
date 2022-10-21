import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

// TODO(mohit): Consolidate with training data generation kafka connector if possible to remove redundant configurations
// and code

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    tierId: number,
    region: string,
    roleArn: string,
    vpcId: string,
    // AWS and S3 configurations
    awsAccessKeyId: pulumi.Input<string>,
    awsSecretAccessKey: pulumi.Input<string>,
    s3BucketName: pulumi.Output<string>,
    protect: boolean,

    // MSK configuration
    mskClusterArn: string,
    mskBootstrapServersIam: string,
    privateSubnetIds: string[],
    mskSgId: string,

    // s3 connect plugin
    s3ConnectPluginArn: string,
    s3ConnectPluginRev: number,

    // s3 worker conf
    s3ConnectWorkerArn: string,
    s3ConnectWorkerRev: number,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    // MSK connector
    const awsProvider = new aws.Provider("offlineaggr-msk-connect-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // create a cloudwatch log group to dump all the logs
    const logGroup = new aws.cloudwatch.LogGroup("offlineaggr-connector-log-group", {
        name: `/aws/msk/t-${input.tierId}-offlineaggr`,
        // retain logs for 5 days
        retentionInDays: 5,
    }, { provider: awsProvider });

    // Create IAM role which has access to the S3 bucket
    const offlineAggrConnectorRole = input.s3BucketName.apply(bucketName => {
        return new aws.iam.Role("offlineaggr-connector-role", {
            namePrefix: `t-${input.tierId}-offlineaggr-connector`,
            description: `IAM role for Offline Aggregate MSK Connector for tier - ${input.tierId}.`,
            assumeRolePolicy: {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "kafkaconnect.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            },
            inlinePolicies: [{
                name: `t-${input.tierId}-offlineaggr-connector-policy`,
                policy: `{
                "Version":"2012-10-17",
                "Statement":[
                    {
                        "Effect":"Allow",
                        "Action": [
                            "s3:ListAllMyBuckets"
                        ],
                        "Resource":"arn:aws:s3:::*"
                    },
                    {
                        "Effect":"Allow",
                        "Action":[
                            "s3:ListBucket",
                            "s3:GetBucketLocation"
                        ],
                        "Resource": [
                            "arn:aws:s3:::${bucketName}",
                            "${input.s3ConnectPluginArn}"
                        ]
                    },
                    {
                        "Effect":"Allow",
                        "Action":[
                            "s3:PutObject",
                            "s3:GetObject",
                            "s3:AbortMultipartUpload",
                            "s3:ListMultipartUploadParts",
                            "s3:ListBucketMultipartUploads"
                        ],
                        "Resource": [
                            "arn:aws:s3:::${bucketName}/*",
                            "${input.s3ConnectPluginArn}/*"
                        ]
                    }
                ]
            }`,
            }, {
                name: `t-${input.tierId}-offlineaggr-connector-msk-access`,
                // TODO(mohit): The resource permissions should be made stricter, but fetching the exact resource names
                // is difficult
                //
                // see - https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-service-execution-role.html
                policy: `{
                "Version":"2012-10-17",
                "Statement":[
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:Connect",
                            "kafka-cluster:DescribeCluster"
                        ],
                        "Resource": "${input.mskClusterArn}"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:ReadData",
                            "kafka-cluster:WriteData",
                            "kafka-cluster:*Topic*"
                        ],
                        "Resource": "*"
                    },
                    {
                        "Effect": "Allow",
                        "Action": [
                            "kafka-cluster:AlterGroup",
                            "kafka-cluster:DescribeGroup"
                        ],
                        "Resource": "*"
                    }
                ]
            }`,
            }],
        }, { provider: awsProvider });
    });

    new aws.mskconnect.Connector("offlineaggr-msk-connector", {
        name: `t-${input.tierId}-offlineaggr-connector`,
        description: `MSK Connector to generate offlineaggr in cold storage for tier ${input.tierId}`,
        kafkaconnectVersion: "2.7.1",
        capacity: {
            // TODO(mohit): Consider configuring Autoscaling if the provisioned workers are not able to keep up
            provisionedCapacity: {
                workerCount: 1,
                mcuCount: 1,  // default
            }
        },
        kafkaClusterClientAuthentication: {
            authenticationType: "IAM",
        },
        kafkaClusterEncryptionInTransit: {
            encryptionType: "TLS",
        },
        connectorConfiguration: {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",
            "topics": `t_${input.tierId}_aggr_offline_transform`,
            "s3.region": input.region,
            "s3.bucket.name": input.s3BucketName,
            "aws.access.key.id": input.awsAccessKeyId,
            "aws.secret.access.key": input.awsSecretAccessKey,
            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "format.class": "io.confluent.connect.s3.format.json.JsonFormat",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "flush.size": "1000000",  // 1M
            // rotate the files every 5 minutes so that the data is flushed constantly
            "rotate.schedule.interval.ms": "300000", // 5M
            "rotate.interval.ms": "3600000",  // 1H
            "topics.dir": "daily",

            // The following are required by TimeBasedPartitioner.
            //
            // see - https://docs.confluent.io/kafka-connectors/s3-sink/current/overview.html#s3-object-formats
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
            // This dictates how frequently time based partitioner creates a new file in path.format. Since we have
            // enabled hourly writes, this needs to match the same. We have limited the upper bound on the number of
            // entries a single file can have using `flush.size`.
            "partition.duration.ms": "3600000",  // 1H
            "locale": "en-US",
            // We will stick with UTC as it was confluent connectors
            "timezone": "UTC"
            // "timestamp.extractor" - this will by default use when the record was processed since not every
            // record we have has timestamp field which could have been used to partition based on the actual record
            // timestamp
        },
        kafkaCluster: {
            apacheKafkaCluster: {
                bootstrapServers: input.mskBootstrapServersIam,
                vpc: {
                    securityGroups: [input.mskSgId],
                    subnets: input.privateSubnetIds,
                },
            }
        },
        workerConfiguration: {
            arn: input.s3ConnectWorkerArn,
            revision: input.s3ConnectWorkerRev,
        },
        serviceExecutionRoleArn: offlineAggrConnectorRole.arn,
        plugins: [{
            customPlugin: {
                arn: input.s3ConnectPluginArn,
                revision: input.s3ConnectPluginRev,
            }
        }],
        logDelivery: {
            workerLogDelivery: {
                cloudwatchLogs: {
                    enabled: true,
                    logGroup: logGroup.name,
                },
            }
        },
        // See if workers need to be configured.
    }, { provider: awsProvider, protect: input.protect });

    const output = pulumi.output({})
    return output
}
