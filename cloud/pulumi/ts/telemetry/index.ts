import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as process from "process";
import * as path from "path";
import * as fs from 'fs';
import * as md5 from 'ts-md5/dist/md5';

export const plugins = {
    "aws": "v5.0.0",
    "kubernetes": "v3.20.1",
}

export type OtelConfig = {
    memoryRequest?: string,
    memoryLimit?: string,

    cpuRequest?: string,
    cpuLimit?: string,
}

export type inputType = {
    planeId: number,
    roleArn: pulumi.Input<string>,
    region: string,
    otelConf?: OtelConfig,
    kubeconfig: pulumi.Output<any>,
    eksClusterName: pulumi.Output<string>,
    nodeInstanceRole: pulumi.Output<string>,
}

export type outputType = {
    otelCollectorEndpoint: string,
    otelCollectorHttpEndpoint: string,
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
                    "ssm:GetParameters",
                    "aps:RemoteWrite"
                ],
                "Resource": "*"
            }
        ]
    }
    `

    const policy = new aws.iam.Policy(`p-${input.planeId}-otel-collector-policy`, {
        namePrefix: `p-${input.planeId}-AWSDistroOpenTelemetryPolicy-`,
        policy: rawPolicyStr,
    }, { provider: awsProvider });


    const attachOtelPolicy = new aws.iam.RolePolicyAttachment(`p-${input.planeId}-otel-instance`, {
        policyArn: policy.arn,
        role: input.nodeInstanceRole,
    }, { provider: awsProvider });

}

// Setup the ADOT (AWS Distro for OpenTelemetry) Collector to collect metrics
// and traces and forward them to cloudwatch.
//
// TODO: consider using HELM charts - https://github.com/open-telemetry/opentelemetry-helm-charts
async function setupAdotCollector(input: inputType, k8sProvider: k8s.Provider) {
    const root = process.env.FENNEL_ROOT!;
    // TODO: Consider refactoring this to avoid creating a config file inside the callback of `apply`.
    // `.apply` should not have any side-effects, but in this case it seems unavoidable
    pulumi.all([input.eksClusterName]).apply(([eksClusterName])=> {
        // Generate a file hash so that any changes in the file, forces pod restart.
        // Without this, any changes to the ConfigMap would not get reflected as the configmap is mounted
        // on pod initialization and any updates to the ConfigMap of the pod are later not reflected.
        const deploymentFilePath = path.join(root, "/deployment/artifacts/otel-deployment.yaml")
        const filehash = md5.Md5.hashStr(fs.readFileSync(deploymentFilePath).toString())
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
                                value: `ClusterName=${eksClusterName},FileHash=${filehash}`,
                            } as k8s.types.output.core.v1.EnvVar)

                            if (input.otelConf !== undefined && container.name === "otel-collector") {
                                if (input.otelConf.memoryLimit !== undefined) {
                                    container.resources.limits.memory = input.otelConf.memoryLimit
                                }
                                if (input.otelConf.memoryRequest !== undefined) {
                                    container.resources.requests.memory = input.otelConf.memoryRequest
                                }
                                if (input.otelConf.cpuLimit !== undefined) {
                                    container.resources.limits.cpu = input.otelConf.cpuLimit
                                }
                                if (input.otelConf.cpuRequest !== undefined) {
                                    container.resources.requests.cpu = input.otelConf.cpuRequest
                                }
                            }

                            return container
                        })
                    }
                    if (obj.kind === "ConfigMap") {
                        let otelAgentConfig = obj.data["otel-agent-config"]
                        otelAgentConfig = otelAgentConfig.replace(
                            new RegExp("%%PLANE_ID%%", 'g'), `plane-${input.planeId}`)
                        obj.data["otel-agent-config"] = otelAgentConfig
                    }
                },
            ],
        }, { provider: k8sProvider, replaceOnChanges: ["*"] })
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
            "plane.id": `plane-${input.planeId}`,
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
        dependsOn: cm,
        // replace the existing pods when there is a configmap change so that effect is immediate and deterministic.
        // previously, we saw that when the pod was restarted (manually or due to scheduling) or when a new pod
        // was scheduled (autoscaler spun up a new node etc), the behavior was different for different nodes
        replaceOnChanges: ["*"],
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
    const output: outputType = {
        // <serviceName>.<namespace>:port - NOTE: only the ipv4 or ipv6 configurations should be provided not URL
        // namespace and port are defined in: rexer/deployment/artifacts/otel-deployment.yaml
        otelCollectorEndpoint: "otel-collector.otel-eks:4317",
        otelCollectorHttpEndpoint: "otel-collector.otel-eks:4318",
    }
    return output
}
