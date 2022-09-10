import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    pprofStoreBucket: string
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

    const output: outputType = {
        pprofStoreBucket: bucketName,
    }
    return output;
}
