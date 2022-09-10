import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    region: string,
    roleArn: pulumi.Input<string>,
    planeId: number,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    bucketName: string,
    userAccessKeyId: string,
    userSecretAccessKey: string,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("connector-sink-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // create s3 bucket
    const bucketName = `p-${input.planeId}-training-data`;
    const bucket = new aws.s3.Bucket("conn-sink-bucket", {
        // OWNER gets full control but no one else has access right.
        // We grant access to the amazon user using user policy instead.
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error.
        forceDestroy: true,
    }, { provider, protect: input.protect });

    // create an AWS user account to authenticate kafka connector to write to S3 bucket
    const user = new aws.iam.User("conn-sink-user", {
        name: `p-${input.planeId}-conn-sink-user`,
        // set path to differentiate this user from the rest of human users
        path: "/conf_conn_user/",
        tags: {
            "managed_by": "fennel.ai",
            "plane": `p-${input.planeId}`,
            "sink": `${bucketName}`,
        }
    }, { provider, dependsOn: bucket });

    // fetch access keys
    const userAccessKey = new aws.iam.AccessKey("conn-sink-access-key", {
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
    const userPolicy = new aws.iam.UserPolicy("conn-sink-user-policy", {
        user: user.name,
        policy: rawPolicyStr,
    }, { provider });

    // create bucket lifecycle policy with expiration
    const bucketlifecycle = new aws.s3.BucketLifecycleConfigurationV2("training-data-bucketlifecyle", {
       bucket: bucketName,
       rules: [{
           id: `p-${input.planeId}-training-data-expiration`,
           status: "Enabled",
           expiration: {
               // expire after 45 days
               days: 45,
               // this is not relevant for us since we don't version objects in a bucket
               expiredObjectDeleteMarker: true,
           }
       }],
    }, { provider: provider });

    const output = pulumi.output({
        bucketName: bucketName,
        userAccessKeyId: userAccessKey.id,
        userSecretAccessKey: userAccessKey.secret,
    })
    return output
}
