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
    // TODO(mohit): See if this should be made a tier specific resource
    planeId: number,
    planeName?: string,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    bucketName: string,
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

    let bucketName: string;
    if (input.planeName) {
        bucketName = `p-${input.planeName}-offline-aggregate-source`
    } else {
        bucketName = `p-${input.planeId}-offline-aggregate-source`
    }

    const bucket = new aws.s3.Bucket(`p-${input.planeId}-offline-aggregate-source-bucket`, {
        // OWNER gets full control but no one else has access right.
        // We grant access to the amazon user using user policy instead.
        acl: "private",
        bucket: bucketName,
        // delete all the objects so that the bucket can be deleted without error.
        forceDestroy: true,
    }, { provider, protect: input.protect })

    const root = process.env["FENNEL_ROOT"]!

    // topk source file
    const topkScriptPath = path.join(root, "pyspark/topk.py");
    const topkFileHash = md5.Md5.hashStr(fs.readFileSync(topkScriptPath).toString())

    const topkFileName = "topk.py";
    const topk = new aws.s3.BucketObject(`p-${input.planeId}-topk-source-object`, {
        bucket: bucket.id,
        key: topkFileName,
        source: new pulumi.asset.FileAsset(topkScriptPath),
        // in case of the file change, force an update
        etag: topkFileHash,
        sourceHash: topkFileHash,
    }, { provider, protect: input.protect });

    // cf source file
    const cfScriptPath = path.join(root, "pyspark/cf.py");
    const cfFileHash = md5.Md5.hashStr(fs.readFileSync(cfScriptPath).toString())

    const cfFileName = "cf.py";
    const cf = new aws.s3.BucketObject(`p-${input.planeId}-cf-source-object`, {
        bucket: bucket.id,
        key: cfFileName,
        source: new pulumi.asset.FileAsset(cfScriptPath),
        // in case of the file change, force an update
        etag: cfFileHash,
        sourceHash: cfFileHash,
    }, { provider, protect: input.protect });

    const sources: Record<string, pulumi.Output<string>> = {
        "topk": topk.key.apply(key => { return `s3://${bucketName}/${key}` }),
        "cf": cf.key.apply(key => { return `s3://${bucketName}/${key}` }),
    }
    return pulumi.output({
        bucketName: bucketName,
        sources: sources,
    })
}
