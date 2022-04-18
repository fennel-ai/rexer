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
    planeId: number
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    modelStorePlaneBucket: string
}

const setupModelStore = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    // create s3 bucket for the plane
    const provider = new aws.Provider("plane-model-store-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucket = new aws.s3.Bucket("plane-model-store-bucket", {
        acl: "private",
        bucket: `p-${input.planeId}-model-store`,
        // delete all the objects so that the bucket can be deleted without error
        forceDestroy: true,
    }, {provider});

    return pulumi.output({
        modelStorePlaneBucket: bucket.id,
    })
}

export default setupModelStore;