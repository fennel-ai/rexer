import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as process from "process";
import * as path from "path";
import { input } from "@pulumi/aws/types";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "aws": "v4.38.0",
    "kubernetes": "v3.16.0",
}

export type inputType = {
    roleArn: string,
    region: string,
    kubeconfig: pulumi.Output<any>,
    eksClusterName: pulumi.Output<string>,
    nodeInstanceRole: pulumi.Output<string>,
}

export type outputType = {}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        roleArn: config.require(nameof<inputType>("roleArn")),
        region: config.require(nameof<inputType>("region")),
        kubeconfig: pulumi.output(config.require(nameof<inputType>("kubeconfig"))),
        eksClusterName: pulumi.output(config.require(nameof<inputType>("eksClusterName"))),
        nodeInstanceRole: pulumi.output(config.require(nameof<inputType>("nodeInstanceRole"))),
    }
}

// TODO: move to library.
class MonitoredDeployment extends k8s.apps.v1.Deployment {
    constructor(name: string,
        args: k8s.apps.v1.Deployment,
        opts?: pulumi.CustomResourceOptions) {
        const metadata = args.spec.template.metadata
        metadata.annotations = metadata.annotations || {};
        metadata.annotations.apply((annotations) => {
            annotations["prometheus.io/scrape"] = "true"
        })
        super(name, args, opts);
    }
}

// TODO: move to library.
class MonitoredReplicaSet extends k8s.apps.v1.ReplicaSet {
    constructor(name: string,
        args: k8s.apps.v1.ReplicaSet,
        opts?: pulumi.CustomResourceOptions) {
        const metadata = args.spec.template.metadata
        metadata.annotations = metadata.annotations || {};
        metadata.annotations.apply((annotations) => {
            annotations["prometheus.io/scrape"] = "true"
        })
        super(name, args, opts);
    }
}

function setupOtelPolicy(input: inputType, awsProvider: aws.Provider) {
    const rawPolicyStr = `{
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:PutLogEvents",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:DescribeLogStreams",
                    "logs:DescribeLogGroups",
                    "cloudwatch:PutMetricData",
                    "xray:PutTraceSegments",
                    "xray:PutTelemetryRecords",
                    "xray:GetSamplingRules",
                    "xray:GetSamplingTargets",
                    "xray:GetSamplingStatisticSummaries",
                    "ssm:GetParameters"
                ],
                "Resource": "*"
            }
        ]
    }
    `

    const policy = new aws.iam.Policy("otel-collector-policy", {
        namePrefix: "AWSDistroOpenTelemetryPolicy-",
        policy: rawPolicyStr,
    }, { provider: awsProvider });


    const attachOtelPolicy = new aws.iam.RolePolicyAttachment("otel-instance", {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: awsProvider });

}

// Setup the ADOT (AWS Distro for OpenTelemetry) Collector to collect metrics
// and traces and forward them to cloudwatch.
async function setupAdotCollector(input: inputType, k8sProvider: k8s.Provider) {
    const root = process.env.FENNEL_ROOT!;
    const deploymentFilePath = path.join(root, "/deployment/artifacts/otel-deployment.yaml")
    // TODO: Consider refactoring this to avoid creating a config file inside the callback of `apply`. 
    // `.apply` should not have any side-effects, but in this case it seems unavoidable
    input.eksClusterName.apply(eksClusterName => {
        const collector = new k8s.yaml.ConfigFile("adot-collector", {
            file: deploymentFilePath,
            transformations: [
                (obj: any, opts: pulumi.CustomResourceOptions) => {
                    if (obj.kind === "Deployment") {
                        let containers = obj.spec.template.spec.containers;
                        obj.spec.template.spec.containers = containers.map((container: any) => {
                            container.env.push({
                                name: "AWS_REGION",
                                value: input.region,
                            } as k8s.types.output.core.v1.EnvVar)
                            container.env.push({
                                name: "OTEL_RESOURCE_ATTRIBUTES",
                                value: `ClusterName=${eksClusterName}`,
                            } as k8s.types.output.core.v1.EnvVar)
                            return container
                        })
                    }
                },
            ],
        }, { provider: k8sProvider })
    })
}

// Setup fluentbit as a daemon-set following instructions at:
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Container-Insights-setup-logs-FluentBit.html
async function setupFluentBit(input: inputType, k8sProvider: k8s.Provider) {
    const ns = new k8s.core.v1.Namespace("cloudwatch-ns", {
        metadata: {
            name: "amazon-cloudwatch",
            labels: {
                "name": "amazon-cloudwatch",
            },
        },
    }, { provider: k8sProvider })

    const cm = new k8s.core.v1.ConfigMap("cluster-info-configmap", {
        data: {
            "cluster.name": input.eksClusterName,
            "read.head": "Off",
            "http.server": "On",
            "http.port": "2020",
            "read.tail": "On",
            "logs.region": input.region,
        },
        metadata: {
            name: "fluent-bit-cluster-info",
            namespace: ns.id,
        }
    }, { provider: k8sProvider })

    const root = process.env.FENNEL_ROOT!;
    const fluentBitConfigPath = path.join(root, "/deployment/artifacts/fluent-bit.yaml")
    const deployment = new k8s.yaml.ConfigFile("fluent-bit-config", {
        file: fluentBitConfigPath,
    }, {
        provider: k8sProvider,
        dependsOn: cm
    })

}

export const setup = async (input: inputType) => {
    const awsProvider = new aws.Provider("tele-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const k8sProvider = new k8s.Provider("tele-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    setupOtelPolicy(input, awsProvider);
    await setupAdotCollector(input, k8sProvider);
    await setupFluentBit(input, k8sProvider);
    const output: outputType = {}
    return output
}

async function run() {
    let output: outputType | undefined;
    // Run the main function only if this program is run through the pulumi CLI.
    // Unfortunately, in that case the argv0 itself is not "pulumi", but the full
    // path of node: e.g. /nix/store/7q04aq0sq6im9a0k09gzfa1xfncc0xgm-nodejs-14.18.1/bin/node
    if (process.argv0 !== 'node') {
        pulumi.log.info("Running...")
        const input: inputType = parseConfig();
        output = await setup(input)
    }
    return output
}


export const output = await run();
