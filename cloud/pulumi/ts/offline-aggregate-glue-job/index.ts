import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    sourceBucket: string,
    storageBucket: pulumi.Output<string>,
    outputBucket: string,
    sourceFiles: Record<string, string>,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    // using a map to easily transform to a string later when this is passed as job arguments
    jobNames: Record<string, string>,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider(`t-${input.tierId}-offline-aggr-glue-provider`, {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // IAM role for the GLUE job with attached policy to read and write to the training data buckets
    // and read from the source code bucket
    const role = new aws.iam.Role(`t-${input.tierId}-offline-aggr-glue-role`, {
        namePrefix: `t-${input.tierId}-offline-aggr-gluerole-`,
        description: "IAM role for AWS GLUE job for offline aggregate ETL",
        assumeRolePolicy: `{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "glue.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }`,
    }, {provider});

    // create inline role policy
    const policy = input.storageBucket.apply(storageBucket => {
        return new aws.iam.RolePolicy(`t-${input.tierId}-offline-aggr-glue-rolepolicy`, {
            name: `t-${input.tierId}-offline-aggr-glue-rolepolicy`,
            role: role,
            policy: `{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect":"Allow",
                    "Action": [
                        "s3:ListAllMyBuckets"
                    ],
                    "Resource":"arn:aws:s3:::*"
                },
                {
                    "Effect":"Allow",
                    "Action": [
                        "s3:ListBucket",
                        "s3:GetBucketLocation"
                    ],
                    "Resource": [
                        "arn:aws:s3:::${storageBucket}",
                        "arn:aws:s3:::${input.sourceBucket}",
                        "arn:aws:s3:::${input.outputBucket}"
                    ]
                },
                {
                    "Effect":"Allow",
                    "Action":[
                        "s3:GetObject"
                    ],
                    "Resource": [
                        "arn:aws:s3:::${input.sourceBucket}/*",
                        "arn:aws:s3:::${storageBucket}/*"
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
                        "arn:aws:s3:::${input.outputBucket}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": "cloudwatch:PutMetricData",
                    "Resource": [
                        "*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogStream",
                        "logs:PutLogEvents"
                    ],
                    "Resource": [
                        "arn:aws:logs:*:*:/aws-glue/*"
                    ]
                }
            ]
        }`,
        }, {provider});
    });

    // create the glue job for topk
    const topkSource = input.sourceFiles["topk"]
    const topkJobName = `t-${input.tierId}-topk`
    const topkJob = input.storageBucket.apply(storageBucket => {
        return new aws.glue.Job(`t-${input.tierId}-topk-gluejob`, {
            name: topkJobName,
            command: {
                scriptLocation: topkSource,
                pythonVersion: "3",
            },
            roleArn: role.arn,
            defaultArguments: {
                '--TIER_ID': `${input.tierId}`,
                '--INPUT_BUCKET': `${storageBucket}`,
                '--OUTPUT_BUCKET': `${input.outputBucket}`,
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-continuous-log-filter': 'false',
                // this is to easily filter out logs from a particular GLUE job
                '--continuous-log-logStreamPrefix': `${topkJobName}`,
                // enable metrics to be reported to CloudWatch
                '--enable-metrics': 'true',
            },
            description: "GLUE job for TopK offline Aggregate",
            glueVersion: "3.0",
            workerType: "G.2X",
            maxRetries: 5,
            numberOfWorkers: 3,  // Has to be >= 2
            timeout: 120,
            executionProperty: {
                maxConcurrentRuns: 50,
            },
        }, {provider});
    });

    // create glue job for cf
    const cfSource = input.sourceFiles["cf"]
    const cfJobName = `t-${input.tierId}-cf`
    const cfJob = input.storageBucket.apply(storageBucket => {
        return new aws.glue.Job(`t-${input.tierId}-cf-gluejob`, {
            name: cfJobName,
            command: {
                scriptLocation: cfSource,
                pythonVersion: "3",
            },
            roleArn: role.arn,
            defaultArguments: {
                '--TIER_ID': `${input.tierId}`,
                '--INPUT_BUCKET': `${storageBucket}`,
                '--OUTPUT_BUCKET': `${input.outputBucket}`,
                '--enable-continuous-cloudwatch-log': 'true',
                '--enable-continuous-log-filter': 'false',
                // this is to easily filter out logs from a particular GLUE job
                '--continuous-log-logStreamPrefix': `${cfJobName}`,
            },
            description: "GLUE job for Collaborative Filtering offline Aggregate",
            glueVersion: "3.0",
            workerType: "G.2X",
            maxRetries: 5,
            numberOfWorkers: 8,  // Has to be >= 2
            timeout: 600,
            executionProperty: {
                maxConcurrentRuns: 20,
            },
        }, {provider});
    });

    const jobNames = pulumi.all([topkJob.name, cfJob.name]).apply(([topkName, cfName]) => {
        return { "topk": topkName, "cf": cfName } as Record<string, string>
    });

    return pulumi.output({
        jobNames: jobNames,
    })
}
