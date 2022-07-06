import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v4.38.1",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    nodeInstanceRole: string,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    pprofStoreBucket: string
}

function setupPprofStoreAccess(provider: aws.Provider, input: inputType, bucketName: string) {
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

    const policy = new aws.iam.Policy(`t-${input.tierId}-node-pprof-storage-policy`, {
        namePrefix: `t-${input.tierId}-NodePprofStoragePolicy-`,
        policy: policyStr,
    }, { provider: provider });

    const attachNodePprofStoragePolicy = new aws.iam.RolePolicyAttachment(`t-${input.tierId}-pprof-storage-policy-attach`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: provider });
}

export const setup = async (input: inputType): Promise<outputType> => {
    // create s3 bucket for the tier
    const provider = new aws.Provider("pprof-bucket-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucketName = `t-${input.tierId}-pprof-store`
    const bucket = new aws.s3.Bucket("tier-pprof-store-bucket", {
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error
        forceDestroy: true,
    }, {provider, protect: input.protect });

    // setup EKS worker node have access to the S3 bucket and the folder
    setupPprofStoreAccess(provider, input, bucketName);

    const output: outputType = {
        pprofStoreBucket: bucketName,
    }
    return output;
}
