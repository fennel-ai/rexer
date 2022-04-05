import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

import * as process from "process";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "aws": "v5.1.0",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    planeId: number,
    sourceBucket: string,
    trainingDataBucket: string,
    script: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    jobName: string,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        tierId: config.requireNumber(nameof<inputType>("tierId")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),
        sourceBucket: config.require(nameof<inputType>("sourceBucket")),
        trainingDataBucket: config.require(nameof<inputType>("trainingDataBucket")),
        script: config.require(nameof<inputType>("script")),
    }
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
                }
            ]
        }`,
    }, {provider});

    // create the glue job
    const job = new aws.glue.Job(`t-${input.tierId}-gluejob`, {
        name: `t-${input.tierId}-training-data-gen`,
        command: {
            scriptLocation: `s3://${input.sourceBucket}/${input.script}`,
            pythonVersion: "3",
        },
        roleArn: role.arn,
        defaultArguments: {
            '--TIER_ID': `${input.tierId}`,
            '--PLANE_ID': `${input.planeId}`,
        },
        description: "GLUE job to transform multiple features and labels files in JSON format to a single Parquet file",
        glueVersion: "3.0",
        workerType: "G.2X",
        maxRetries: 5,
        numberOfWorkers: 2,  // Has to be >= 2
        timeout: 60,  // it should not take more than 60 minutes to transform the json files
    }, {provider});

    // create a trigger to run this job every day
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

    return pulumi.output({
        jobName: job.name,
    })
}

async function run() {
    let output: pulumi.Output<outputType> | undefined;
    // Run the main function only if this program is run through the pulumi CLI.
    // Unfortunately, in that case the argv0 itself is not "pulumi", but the full
    // path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
    if (process.argv0 !== 'node') {
        pulumi.log.info("Running...")
        const input: inputType = parseConfig();
        output = await setup(input)
    }
    return output
}


export const output = await run();