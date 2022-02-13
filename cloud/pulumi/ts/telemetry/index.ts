import * as aws from "@pulumi/aws";
import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";

export = async () => {
    setupOtelPolicy();
    await setupAdotCollector();
    await setupFluentBit();
    return { MonitoredDeployment, MonitoredReplicaSet }
}

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

function setupOtelPolicy() {
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
    });


    const config = new pulumi.Config();
    const attachOtelPolicy = new aws.iam.RolePolicyAttachment("otel-instance", {
        policyArn: policy.arn,
        role: config.require("instanceRole"),
    });

}

// Setup the ADOT (AWS Distro for OpenTelemetry) Collector to collect metrics
// and traces and forward them to cloudwatch.
async function setupAdotCollector() {
    const region = (await aws.getRegion()).name;
    const config = new pulumi.Config();
    const eksClusterName = config.require("eksClusterName");
    const collector = new k8s.yaml.ConfigFile("adot-collector", {
        file: "otel-deployment.yaml",
        transformations: [
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Deployment") {
                    let containers = obj.spec.template.spec.containers;
                    obj.spec.template.spec.containers = containers.map((container: any) => {
                        container.env.push({
                            name: "AWS_REGION",
                            value: region,
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
    })
}

// Setup fluentbit as a daemon-set following instructions at:
// https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/Container-Insights-setup-logs-FluentBit.html
async function setupFluentBit() {
    const ns = new k8s.core.v1.Namespace("cloudwatch-ns", {
        metadata: {
            name: "amazon-cloudwatch",
            labels: {
                "name": "amazon-cloudwatch",
            },
        },
    })

    const region = (await aws.getRegion()).name;
    const config = new pulumi.Config();
    const eksClusterName = config.require("eksClusterName");
    const cm = new k8s.core.v1.ConfigMap("cluster-info-configmap", {
        data: {
            "cluster.name": eksClusterName,
            "read.head": "Off",
            "http.server": "On",
            "http.port": "2020",
            "read.tail": "On",
            "logs.region": region,
        },
        metadata: {
            name: "fluent-bit-cluster-info",
            namespace: ns.id,
        }
    })

    const deployment = new k8s.yaml.ConfigFile("fluent-bit-config", {
        file: "fluent-bit.yaml",
    }, { dependsOn: cm })

}