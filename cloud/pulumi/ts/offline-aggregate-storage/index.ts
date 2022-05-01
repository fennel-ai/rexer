import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v4.38.1"
}

// TODO(mohit): Consolidate with training data generation kafka connector if possible to remove redundant configurations
// and code

export type inputType = {
    region: string,
    roleArn: string,
    planeId: number,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    bucketName: string
    userAccessKeyId: string,
    userSecretAccessKey: string,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("offline-aggregate-storage-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucketName = `p-${input.planeId}-offline-aggregate-storage`
    const bucket = new aws.s3.Bucket(`p-${input.planeId}-offline-aggregate-storage`, {
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error
        forceDestroy: true,
    }, {provider});

    // setup AWS user account with access to this bucket. This user access is used by kafka connector
    const user = new aws.iam.User(`p-${input.planeId}-offline-aggr-user`, {
        name: `p-${input.planeId}-offline-aggr-user`,
        // set path to differentiate this user from the rest of human users
        path: "/conf_conn_user/",
        tags: {
            "managed_by": "fennel.ai",
            "plane": `p-${input.planeId}`,
            "sink": `${bucketName}`,
        }
    }, { provider, dependsOn: bucket });

    // fetch access keys
    const userAccessKey = new aws.iam.AccessKey(`p-${input.planeId}-offline-aggr-access-key`, {
        user: user.name
    }, { provider });

    // https://docs.confluent.io/cloud/current/connectors/cc-s3-sink.html
    const rawPolicyStr = `{
        "Version":"2012-10-17",
        "Statement":[
            {
                "Effect":"Allow",
                "Action": [
                    "s3:ListAllMyBuckets"
                ],
                "Resource":"arn:aws:s3:::*"
            },
            {
                "Effect":"Allow",
                "Action":[
                    "s3:ListBucket",
                    "s3:GetBucketLocation"
                ],
                "Resource":"arn:aws:s3:::${bucketName}"
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
                "Resource":"arn:aws:s3:::${bucketName}/*"
            }
        ]
    }`;
    const userPolicy = new aws.iam.UserPolicy(`p-${input.planeId}-offline-aggr-user-policy`, {
        user: user.name,
        policy: rawPolicyStr,
    }, { provider });

    return pulumi.output({
        bucketName: bucketName,
        userAccessKeyId: userAccessKey.id,
        userSecretAccessKey: userAccessKey.secret,
    })
}
