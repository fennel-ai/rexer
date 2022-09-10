import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

import * as path from "path";
import * as process from "process";
import * as fs from 'fs';
import * as md5 from 'ts-md5/dist/md5';

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
    scriptSourceBucket: string,
    scriptPath: string,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    // create a s3 bucket with the glue job python script to run
    const provider = new aws.Provider("glue-source-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucketName = `p-${input.planeId}-gluejob-source`
    const bucket = new aws.s3.Bucket("glue-source-bucket", {
        // OWNER gets full control but no one else has access right.
        // We grant access to the amazon user using user policy instead.
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error.
        forceDestroy: true,
    }, { provider, protect: input.protect })

    const root = process.env["FENNEL_ROOT"]!
    const scriptPath = path.join(root, "tools/aws_glue_parquet_transform.py");
    const fileHash = md5.Md5.hashStr(fs.readFileSync(scriptPath).toString())

    const object = new aws.s3.BucketObject("py-source-object", {
        bucket: bucket.id,
        key: "aws_glue_parquet_transforms.py",
        source: new pulumi.asset.FileAsset(scriptPath),
        // in case of the file change, force an update
        etag: fileHash,
        sourceHash: fileHash,
    }, { provider, protect: input.protect })

    return pulumi.output({
        scriptSourceBucket: bucketName,
        scriptPath: object.key,
    })
}
