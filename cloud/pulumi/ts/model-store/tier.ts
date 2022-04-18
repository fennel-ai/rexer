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
    planeModelStoreBucket: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    tierModelStoreDir: string
}

function setupModelStoreAccess(provider: aws.Provider, input: inputType, folderName: pulumi.Output<string>) {
    const policyStr = folderName.apply(folder => {
        return `{
          "Version": "2012-10-17",
          "Statement": [
            {
              "Effect": "Allow",
              "Action": [
                "s3:ListBucket",
                "s3:ListBucket2"
              ],
              "Resource": "arn:aws:s3:::${input.planeModelStoreBucket}"
            },
            {
              "Effect": "Allow",
              "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:DeleteObject"
              ],
              "Resource": "arn:aws:s3:::${input.planeModelStoreBucket}/${folder}/*"
            }
          ]
        }
        `
    });

    const policy = new aws.iam.Policy(`t-${input.tierId}-node-model-storage-policy`, {
        namePrefix: `t-${input.tierId}-NodeModelStoragePolicy-`,
        policy: policyStr,
    }, { provider: provider });

    const attachNodeModelStoragePolicy = new aws.iam.RolePolicyAttachment(`t-${input.tierId}-node-model-storage-policy-attach`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: provider });
}

const setupModelStore = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    // create s3 bucket for the plane
    const provider = new aws.Provider("tier-model-store-dir-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // The object being created here is a "folder", under which the models will be saved
    const object = new aws.s3.BucketObject("tier-model-store-dir", {
        bucket: input.planeModelStoreBucket,
        acl: "private",
        key: `t-${input.tierId}/`,
        // Destroy the folder and all the objects (models in this case) underneath it
        forceDestroy: true,
    }, {provider});

    // setup EKS worker node have access to the S3 bucket and the folder
    setupModelStoreAccess(provider, input, object.key);

    return pulumi.output({
        tierModelStoreDir: object.key,
    })
}

export default setupModelStore;