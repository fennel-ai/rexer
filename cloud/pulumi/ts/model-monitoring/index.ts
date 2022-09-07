import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";
import process from "process";
import path from "path";
import childProcess from "child_process";
import * as uuid from "uuid";
import * as util from "../lib/util";

export const plugins = {
    "kubernetes": "v3.20.1",
    "aws": "v5.1.0"
}

export type inputType = {
    planeId: number,
    region: string,
    roleArn: pulumi.Input<string>,
    kubeconfig: pulumi.Input<any>,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const awsProvider = new aws.Provider("model-monitoring-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`p-${input.planeId}-model-monitoring-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    const repoPolicy = new aws.ecr.LifecyclePolicy(`p-${input.planeId}-model-monitoring-repo-policy`, {
        repository: repo.name,
        policy: {
            rules: [{
                // sets the order in which rules are applied; this rule will be applied first
                rulePriority: 1,
                description: "Policy to expire images after 120 days",
                selection: {
                    // since we don't deterministically know the tag prefix, we use "any" -> both tagged and untagged
                    // images are considered
                    tagStatus: "any",
                    // limits since when the image was pushed
                    countType: "sinceImagePushed",
                    // set 30 days as the ttl
                    countUnit: "days",
                    countNumber: 30,
                },
                action: {
                    type: "expire"
                },
            }],
        }
    }, { provider: awsProvider });

    const root = process.env["FENNEL_ROOT"]!;
    const dockerfile = path.join(root, 'dockerfiles/model_monitoring.dockerfile');
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageNameWithTag = repo.repositoryUrl.apply(iName => {
        return `${iName}:${hashId}-${uuid.v4()}`;
    });

    const imgBuildPush = util.BuildMultiArchImage("model-monitoring-img", root, dockerfile, imageNameWithTag);

    const k8sProvider = new k8s.Provider("model-monitoring-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: "fennel",
    });

    const name = "model-monitoring";
    const appLabels = { app: name };
    const metricsPort = 8112;

    const appDep = new k8s.apps.v1.Deployment("model-monitoring-deployment", {
        metadata: {
            name: name,
        },
        spec: {
            selector: { matchLabels: appLabels },
            template: {
                metadata: {
                    labels: appLabels,
                    annotations: {
                        // we don't need linkerd for this deployment
                        "linkerd.io/inject": "disabled",
                        "prometheus.io/scrape": "true",
                        "prometheus.io/port": metricsPort.toString(),
                    }
                },
                spec: {
                    containers: [{
                        name: name,
                        image: imageNameWithTag,
                        imagePullPolicy: "Always",
                        args: [
                            `--metrics_port=${metricsPort}`
                        ],
                        ports: [
                            {
                                containerPort: metricsPort,
                                protocol: "TCP",
                            },
                        ],
                        resources: {
                            requests: {
                                "cpu": "200m",
                                "memory": "500M",
                            },
                            limits: {
                                "cpu": "1000m",
                                "memory": "2G",
                            }
                        },
                    },],
                },
            },
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true, dependsOn: imgBuildPush });

    return pulumi.output({})
}
