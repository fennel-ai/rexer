import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

import * as path from "path";
import * as process from "process";
import * as fs from 'fs';
import * as md5 from 'ts-md5/dist/md5';

export const plugins = {
    "aws": "v4.38.1",
}

export type inputType = {
    region: string,
    roleArn: string,
    // TODO(mohit): See if this should be made a tier specific resource
    planeId: number,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    bucket: string,
    sources: Record<string, string>
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("offline-aggregate-source-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const bucketName = `p-${input.planeId}-offline-aggregate-source`
    const bucket = new aws.s3.Bucket(`p-${input.planeId}-offline-aggregate-source-bucket`, {
        // OWNER gets full control but no one else has access right.
        // We grant access to the amazon user using user policy instead.
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error.
        forceDestroy: true,
    }, { provider })

    const root = process.env["FENNEL_ROOT"]!
    const scriptPath = path.join(root, "pyspark/topk.py");
    const fileHash = md5.Md5.hashStr(fs.readFileSync(scriptPath).toString())

    const topk = new aws.s3.BucketObject(`p-${input.planeId}-topk-source-object`, {
        bucket: bucket.id,
        key: "topk.py",
        source: new pulumi.asset.FileAsset(scriptPath),
        // in case of the file change, force an update
        etag: fileHash,
        sourceHash: fileHash,
    }, { provider })

    const output = pulumi.output({
        bucket: bucketName,
        sources: {
            "topk": `s3://${bucketName}/${topk.key}`
        }
    })
    return output
}
