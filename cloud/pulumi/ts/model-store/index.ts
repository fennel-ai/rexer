import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

export const plugins = {
    "aws": "v4.38.1",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    nodeInstanceRole: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    modelStoreBucket: string
}

function setupModelStoreAccess(provider: aws.Provider, input: inputType, bucketName: string) {
    const policyStr = `{
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:ListBucket",
                "s3:ListBucket2"
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

    const policy = new aws.iam.Policy(`t-${input.tierId}-node-model-storage-policy`, {
        namePrefix: `t-${input.tierId}-NodeModelStoragePolicy-`,
        policy: policyStr,
    }, { provider: provider });

    const attachNodeModelStoragePolicy = new aws.iam.RolePolicyAttachment(`t-${input.tierId}-node-model-storage-policy-attach`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: provider });
}

export const setup = async (input: inputType): Promise<outputType> => {
    // create s3 bucket for the tier
    const provider = new aws.Provider("tier-model-store-dir-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucketName = `t-${input.tierId}-model-store`
    const bucket = new aws.s3.Bucket("tier-model-store-bucket", {
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error
        forceDestroy: true,
    }, {provider});

    // setup EKS worker node have access to the S3 bucket and the folder
    setupModelStoreAccess(provider, input, bucketName);

    const output: outputType = {
        modelStoreBucket: bucketName,
    }
    return output;
}
