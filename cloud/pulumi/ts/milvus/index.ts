import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";

export const plugins = {
    "kubernetes": "v3.19.0",
    "aws": "v5.4.0"
}

export type inputType = {
    kubeconfig: pulumi.Input<any>,
    region: string,
    roleArn: string,
    planeId: number,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {

    const awsProvider = new aws.Provider("milvus-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const milvusUser = new aws.iam.User(`milvus-p-${input.planeId}`, {
        name: `p-${input.planeId}-milvus-user`,
    }, { provider: awsProvider })

    const bucketName = `p-${input.planeId}-milvus-data`
    const milvusBucket = new aws.s3.Bucket(`milvus-bucket-${input.planeId}`, {
        bucket: bucketName,
        policy: {
            Version: "2012-10-17",
            Statement: [
                {
                    Effect: "Allow",
                    Principal: {
                        AWS: milvusUser.arn,
                    },
                    Resource: [
                        `arn:aws:s3:::${bucketName}`,
                        `arn:aws:s3:::${bucketName}/*`,
                    ],
                    Action: [
                        "s3:*"
                    ]
                }
            ]
        }
    }, { provider: awsProvider })

    const milvusAccessKey = new aws.iam.AccessKey(`milvus-p-${input.planeId}-access-key`, {
        user: milvusUser.name,
    }, { provider: awsProvider })

    const k8sProvider = new k8s.Provider("prom-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    const milvusNs = new k8s.core.v1.Namespace("milvus-ns", {
        metadata: {
            name: "milvus",
        }
    }, { provider: k8sProvider })

    const milvus = new k8s.helm.v3.Release("milvus", {
        repositoryOpts: {
            "repo": "https://milvus-io.github.io/milvus-helm/",
        },
        chart: "milvus",
        namespace: "milvus",
        values: {
            "cluster": {
                "enabled": true,
            },
            "externalS3": {
                "enabled": true,
                "host": `s3.${input.region}.amazonaws.com`,
                "post": 80,
                "accessKey": milvusAccessKey.id,
                "secretKey": milvusAccessKey.secret,
                "bucketName": milvusBucket.id,
            },
            "minio": {
                "enabled": false,
            },
            "service": {
                "type": "LoadBalancer",
            },
        }
    }, { provider: k8sProvider })

    const output = pulumi.output({})

    return output
}
