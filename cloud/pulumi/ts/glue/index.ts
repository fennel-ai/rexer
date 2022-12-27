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
    trainingDataBucket: string,
    script: string,

    enableTrainingDatasetGenerationJobs?: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    jobName: string,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("glue-job-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // IAM role for the GLUE job with attached policy to read and write to the training data buckets
    // and read from the source code bucket
    const role = new aws.iam.Role(`t-${input.tierId}-glue-role`, {
        namePrefix: `t-${input.tierId}-gluerole-`,
        description: "IAM role for AWS GLUE job",
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
    const policy = new aws.iam.RolePolicy(`t-${input.tierId}-glue-rolepolicy`, {
        name: `t-${input.tierId}-glue-rolepolicy`,
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
                        "arn:aws:s3:::${input.trainingDataBucket}",
                        "arn:aws:s3:::${input.sourceBucket}"
                    ]
                },
                {
                    "Effect":"Allow",
                    "Action":[
                        "s3:GetObject"
                    ],
                    "Resource": [
                        "arn:aws:s3:::${input.sourceBucket}/*"
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
                        "arn:aws:s3:::${input.trainingDataBucket}/*"
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

    // create the glue job
    const jobName = `t-${input.tierId}-training-data-gen`;
    const job = new aws.glue.Job(`t-${input.tierId}-gluejob`, {
        name: jobName,
        command: {
            scriptLocation: `s3://${input.sourceBucket}/${input.script}`,
            pythonVersion: "3",
        },
        roleArn: role.arn,
        defaultArguments: {
            '--TIER_ID': `${input.tierId}`,
            '--BUCKET_NAME': `${input.trainingDataBucket}`,
            '--enable-continuous-cloudwatch-log': 'true',
            '--enable-continuous-log-filter': 'false',
            // this is to easily filter out logs from a particular GLUE job
            '--continuous-log-logStreamPrefix': `${jobName}`,
            // enable metrics to be reported to CloudWatch
            '--enable-metrics': 'true',
        },
        description: "GLUE job to transform multiple features and labels files in JSON format to a single Parquet file",
        glueVersion: "3.0",
        workerType: "G.2X",
        maxRetries: 5,
        numberOfWorkers: 5,  // Has to be >= 2
        timeout: 180,  // set a higher timeout for this job since it is run only once in a day
    }, {provider});

    // create a trigger to run this job every day
    if (input.enableTrainingDatasetGenerationJobs) {
        const trigger = new aws.glue.Trigger(`t-${input.tierId}-scheduled-trigger`, {
            // https://docs.aws.amazon.com/glue/latest/dg/monitor-data-warehouse-schedule.html
            // run every at 00:15 UTC
            schedule: "cron(15 0 * * ? *)",
            type: "SCHEDULED",
            actions: [{
                jobName: job.name,
            }],
            description: "Triggers GLUE job to transform features and labels from in JSON format to Parquet",
        }, {provider});
    }

    return pulumi.output({
        jobName: job.name,
    })
}
