import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v5.1.0",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    sourceBucket: string,
    storageBucket: string,
    outputBucket: string,
    sourceFiles: Record<string, string>,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    jobName: string,
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
    const policy = new aws.iam.RolePolicy(`t-${input.tierId}-offline-aggr-glue-rolepolicy`, {
        name: `t-${input.tierId}-offline-aggr-glue-rolepolicy`,
        role: role,
        // TODO(mohit): Make access to storageBucket a bit more granular
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
                        "arn:aws:s3:::${input.storageBucket}",
                        "arn:aws:s3:::${input.sourceBucket}"
                    ]
                },
                {
                    "Effect":"Allow",
                    "Action":[
                        "s3:GetObject"
                    ],
                    "Resource": [
                        "arn:aws:s3:::${input.sourceBucket}/*",
                        "arn:aws:s3:::${input.storageBucket}/*"
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
                }
            ]
        }`,
    }, {provider});

    // create the glue job for topk
    const topkSource = input.sourceFiles["topk"]
    const topkJob = new aws.glue.Job(`t-${input.tierId}-gluejob`, {
        name: `t-${input.tierId}-topk`,
        command: {
            scriptLocation: topkSource,
            pythonVersion: "3",
        },
        roleArn: role.arn,
        defaultArguments: {
            '--TIER_ID': `${input.tierId}`,
        },
        description: "GLUE job to transform multiple features and labels files in JSON format to a single Parquet file",
        glueVersion: "3.0",
        workerType: "G.2X",
        maxRetries: 5,
        numberOfWorkers: 2,  // Has to be >= 2
        timeout: 60,  // it should not take more than 60 minutes to transform the json files
    }, {provider});

    return pulumi.output({
        jobName: topkJob.name,
    })
}
