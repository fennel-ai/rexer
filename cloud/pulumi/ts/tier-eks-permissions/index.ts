import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    region: string,
    tierId: number,
    roleArn: string,
    nodeInstanceRole: string,
    modelStoreBucket: string,
    pprofBucket: string,
    offlineAggregateOutputBucket: string,
    queryServerShadowBucket?: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("tier-eks-instance-iam-policy-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });
    let bucketArns: string[] = [
        `arn:aws:s3:::${input.modelStoreBucket}`,
        `arn:aws:s3:::${input.pprofBucket}`,
        `arn:aws:s3:::${input.offlineAggregateOutputBucket}`
    ];
    let bucketObjectArns: string[] = [
        `arn:aws:s3:::${input.modelStoreBucket}/*`,
        `arn:aws:s3:::${input.pprofBucket}/*`,
        `arn:aws:s3:::${input.offlineAggregateOutputBucket}/*`
    ];
    if (input.queryServerShadowBucket !== undefined) {
        // TODO(mohit): Move this to an independent folder if this is required in the future
        const bucket = new aws.s3.Bucket(`query-server-shadow-reqs-storage`, {
            acl: "private",
            bucket: input.queryServerShadowBucket!,
            // delete all the objects so that the bucket can be deleted without error
            forceDestroy: true,
        }, { provider });
        bucketArns.push(`arn:aws:s3:::${input.queryServerShadowBucket!}`);
        bucketObjectArns.push(`arn:aws:s3:::${input.queryServerShadowBucket!}/*`);
    }

    const name = `t-${input.tierId}-tier-eks-instance-policy`
    const policy = new aws.iam.Policy(name, {
        namePrefix: name,
        policy: JSON.stringify({
            Version: "2012-10-17",
            Statement: [
                {
                    Effect: "Allow",
                    Action: [
                        "s3:ListBucket"
                    ],
                    Resource: bucketArns,
                },
                {
                    Effect: "Allow",
                    Action: [
                        "s3:PutObject",
                        "s3:GetObject",
                    ],
                    Resource: bucketObjectArns,
                },
                {
                    Effect: "Allow",
                    Action: [
                        "s3:DeleteObject"
                    ],
                    Resource: bucketObjectArns,
                },
                {
                    Effect: "Allow",
                    Action: [
                        "glue:CreateTrigger",
                        "glue:ListTriggers",
                        "glue:DeleteTrigger"
                    ],
                    Resource: "*"
                }
            ],
        }),
    }, { provider: provider });

    const attachPolicy = new aws.iam.RolePolicyAttachment(`${name}-attach`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: provider });

    return pulumi.output({})
}
