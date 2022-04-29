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
    nodeInstanceRole: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    // using a map to easily transform to a string later when this is passed as job arguments
    jobNames: Map<string, string>,
}

function setupNodeTriggerAccess(provider: aws.Provider, input: inputType) {
    // setup permissions for the EKS node instance to create triggers; since the API server can create as many triggers
    // as user configures, we set `resource`: "*"
    //
    // TODO(mohit): See if granular permissions could be created e.g. Resource: "arn:aws:glue:*:030813887342:trigger/t-XXX-*"
    // and make API server create triggers with t-XXX as the prefix
    const policyStr = `{
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "glue:CreateTrigger",
                "glue:ListTriggers",
                "glue:DeleteTrigger"
              ],
              "Resource": "*"
            }
          ]
        }
        `
    const policy = new aws.iam.Policy(`t-${input.tierId}-node-trigger-crud-policy`, {
        namePrefix: `t-${input.tierId}-NodeTriggerCRUD-`,
        policy: policyStr,
    }, { provider: provider });
    const attachNodeTriggerCrudPolicy = new aws.iam.RolePolicyAttachment(`t-${input.tierId}-node-trigger-crud-policy-attach`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: provider });
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

    // setup EKS worker node have access to CRUD GLUE triggers
    setupNodeTriggerAccess(provider, input);

    // create the glue job for topk
    const topkSource = input.sourceFiles["topk"]
    const topkJobName = `t-${input.tierId}-topk`
    const topkJob = new aws.glue.Job(`t-${input.tierId}-gluejob`, {
        name: topkJobName,
        command: {
            scriptLocation: topkSource,
            pythonVersion: "3",
        },
        roleArn: role.arn,
        defaultArguments: {
            '--TIER_ID': `${input.tierId}`,
            '--INPUT_BUCKET': `${input.storageBucket}`,
            '--OUTPUT_BUCKET': `${input.outputBucket}`,
        },
        description: "GLUE job to transform multiple features and labels files in JSON format to a single Parquet file",
        glueVersion: "3.0",
        workerType: "G.2X",
        maxRetries: 5,
        numberOfWorkers: 2,  // Has to be >= 2
        timeout: 60,  // it should not take more than 60 minutes to transform the json files
    }, {provider});

    let jobNames = new Map<string, string>;
    jobNames.set("topk", topkJobName)

    return pulumi.output({
        jobNames: jobNames,
    })
}
