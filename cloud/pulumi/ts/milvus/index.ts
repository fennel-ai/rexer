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
    roleArn: pulumi.Input<string>,
    planeId: number
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    endpoint: string,
}

export const setup = async (input: inputType): Promise<outputType> => {

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

    const k8sProvider = new k8s.Provider("milvus-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    const milvus = new k8s.helm.v3.Release("milvus", {
        repositoryOpts: {
            "repo": "https://milvus-io.github.io/milvus-helm/",
        },
        chart: "milvus",
        name: "milvus",
        createNamespace: true,
        namespace: "milvus",
        // See: https://github.com/milvus-io/milvus-helm/blob/master/charts/milvus/values.yaml
        values: {
            "cluster": {
                "enabled": true,
            },
            "etcd": {
                "image": {
                    "tag": "3.5.1",
                    "pullPolicy": "IfNotPresent"
                },
                // Set node selector (affinity) explicitly for etcd as this is installed as a dependency to milvus
                // and setting a global configuration on milvus does not apply here.
                //
                // https://github.com/kubernetes/kubernetes/issues/57838
                "nodeSelector": {
                    "kubernetes.io/arch": "amd64",
                }
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
                // See "ingress" project to see why we configure the load balancer
                // with the following params.
                "annotations": {
                    "service.beta.kubernetes.io/aws-load-balancer-type": "external",
                    "service.beta.kubernetes.io/aws-load-balancer-nlb-target-type": "instance",
                    "service.beta.kubernetes.io/aws-load-balancer-scheme": "internal",
                }
            },
            // Run attu in port-forward mode.
            "attu": {
                "enabled": true,
                "ingress": {
                    "enabled": false,
                },
            },
            // TODO(mohit): Configure this per-component once helm chart supports this
            // see: https://github.com/milvus-io/milvus-helm/issues/339
            "metrics": {
                "enabled": false,
            },
            // Milvus services are not arm64 compatible. Set this at the global level.
            "nodeSelector": {
                "kubernetes.io/arch": "amd64",
            },
            // Set node selector (affinity) explicitly for pulsar as this is installed as a dependency to milvus
            // and setting a global configuration on milvus does not apply here.
            //
            // Pulsar does not allow configuring a global node selector, instead we set this on every component
            // which is enabled by milvus. Filed here - https://github.com/apache/pulsar-helm-chart/issues/275
            //
            // List of enabled components - https://github.com/milvus-io/milvus-helm/blob/master/charts/milvus/values.yaml#L468
            "pulsar": {
                "enabled": true,
                "zookeeper": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                    },
                },
                "bookkeeper": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                    },
                },
                "autorecovery": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                    },
                },
                "broker": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                    },
                },
                "proxy": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                    },
                },
            },
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    return {
        endpoint: "milvus.milvus:19530"
    }
}
