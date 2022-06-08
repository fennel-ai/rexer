import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v4.38.1"
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    nodeInstanceRole: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    bucketName: string
}

function setupOfflineBucketStoreAccess(provider: aws.Provider, input: inputType, bucketName: string) {
    const policyStr = `{
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:ListBucket"
              ],
              "Resource": "arn:aws:s3:::${bucketName}"
            },
            {
              "Effect": "Allow",
              "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
              ],
              "Resource": "arn:aws:s3:::${bucketName}/*"
            }
          ]
        }
    `

    const policy = new aws.iam.Policy(`t-${input.tierId}-node-offline-aggr-output-policy`, {
        namePrefix: `t-${input.tierId}-NodeOfflineAggrOutputPolicy-`,
        policy: policyStr,
    }, { provider: provider });

    const attachNodeOfflineAggrOutputPolicy = new aws.iam.RolePolicyAttachment(`t-${input.tierId}-node-offline-aggr-output-policy-attach`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: provider });
}

export const setup = async (input: inputType): Promise<outputType> => {
    const provider = new aws.Provider("offline-aggregate-output-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucketName = `t-${input.tierId}-offline-aggregate-output`
    const bucket = new aws.s3.Bucket(`p-${input.tierId}-offline-aggregate-output`, {
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error
        forceDestroy: true,
    }, {provider});

    // setup EKS worker node have access to the S3 bucket and the folder
    setupOfflineBucketStoreAccess(provider, input, bucketName);

    return { bucketName: bucketName }
}
