import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as process from "process";

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "aws": "v5.1.0",
    "kubernetes": "v3.18.0"
}

export type inputType = {
    useAMP: boolean,
    kubeconfig: pulumi.Input<any>,
    apiServer: pulumi.Input<string>,
    region: string,
    roleArn: string,
    planeId: number,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    arn: string,
    prometheusWriteEndpoint: string,
    prometheusQueryEndpoint: string,
}

const prometheusScrapeConfigs = {
    "scrape_configs": [{
        "job_name": "kubernetes-pods",
        "sample_limit": 10000, 
        "kubernetes_sd_configs": [{
            "role": "pod"
        }],
        "relabel_configs": [{
            "action": "keep",
            "regex": true,
            "source_labels": ["__meta_kubernetes_pod_annotation_prometheus_io_scrape"]
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_annotation_prometheus_io_port", "__address__"],
            "regex": "([^:]+)(?::\d+)?;(\d+)",
            "replacement": "$$1:$$2",
            "target_label": "__address__"
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_namespace"],
            "target_label": "Namespace",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_name"],
            "target_label": "PodName",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_container_name"],
            "target_label": "ContainerName",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_controller_name"],
            "target_label": "PodControllerName",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_controller_kind"],
            "target_label": "PodControllerKind",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_phase"],
            "target_label": "PodPhase",
        }, {
            "action": "drop",
            "source_labels": ["__meta_kubernetes_pod_container_name"],
            "regex": "(linkerd-init|linkerd-proxy)",
        }],
        // fails with: "str `__name__` into model.LabelNames"
        // "metric_relabel_configs": [{
        //     "action": "drop",
        //     "source_labels": ["__name__"],
        //     "regex": "go_gc_duration_seconds.*"
        //   }]
    }, {
        "job_name": "kubernetes-service-endpoints",
        "kubernetes_sd_configs": [{
            "role": "endpoints"
        }],
        "relabel_configs": [{
            "action": "keep",
            "regex": true,
            "source_labels": ["__meta_kubernetes_service_annotation_prometheus_io_scrape"]
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_service_annotation_prometheus_io_port", "__address__"],
            "regex": "([^:]+)(?::\d+)?;(\d+)",
            "replacement": "$$1:$$2",
            "target_label": "__address__"
        }, {
            "action": "labelmap",
            "regex": "__meta_kubernetes_pod_label_(.+)",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_namespace"],
            "target_label": "Namespace",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_service_name"],
            "target_label": "Service",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_node_name"],
            "target_label": "kubernetes_node",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_name"],
            "target_label": "PodName",
        }, {
            "action": "replace",
            "source_labels": ["__meta_kubernetes_pod_container_name"],
            "target_label": "ContainerName",
        }],
        // Fails with: str `__name__` into model.LabelNames
        // "metric_relabel_configs": [{
        //     "source_labels": ["__name__"],
        //     "regex": "go_gc_duration_seconds.*",
        //     "action": "drop",
        //   }]
    }],
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        useAMP: config.requireBoolean(nameof<inputType>("useAMP")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        apiServer: config.require(nameof<inputType>("apiServer")),
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        planeId: config.requireNumber(nameof<inputType>("planeId")),
    }
}

export const setupAMP = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const awsProvider = new aws.Provider("prom-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const workspaceName = `p-${input.planeId}-prom`
    const prom = new aws.amp.Workspace(workspaceName, {
        alias: workspaceName,
    }, {provider: awsProvider})

    const arn = prom.arn
    const prometheusWriteEndpoint = prom.prometheusEndpoint.apply(endpoint => {
        // endpoint ends with `/`.
        return `${endpoint}api/v1/remote_write`
    }) 
    const prometheusQueryEndpoint = prom.prometheusEndpoint.apply(endpoint => {
        // endpoint ends with `/`.
        return `${endpoint}api/v1/query`
    })

    const output = pulumi.output({
        arn,
        prometheusWriteEndpoint,
        prometheusQueryEndpoint,
    })
    return output
}

export const setupPrometheus = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const k8sProvider = new k8s.Provider("prom-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    // By default prometheus chart creates 5 pods - alertmanager, node-exporter, pushgateway, server and node-state-metrics.
    // We disable alertmanager - since we will create alerts using grafana.
    // We disable node-exporter - node-exporter exports node and OS level metrics which are too granular for us now.
    //  Instead we will use node-state-metrics
    // We disable pushgateway - this prometheus service is useful when metrics need to be collected from
    //  short lived jobs which is not the case for us.
    const prometheusRelease = new k8s.helm.v3.Release("prometheus", {
        repositoryOpts: {
            "repo": "https://prometheus-community.github.io/helm-charts"
        },
        chart: "prometheus",
        values: {
            "serviceAccounts": {
                "alertmanager": {
                    "create": false
                },
                "pushgateway": {
                    "create": false
                },
                "nodeExporter": {
                    "create": false
                }
            },
            "alertmanager": {
                "enabled": false
            },
            "pushgateway": {
                "enabled": false
            },
            "nodeExporter": {
                "enabled": false
            },
            // Set service type as LoadBalancer so that AWS LBC creates a corresponding
            // NLB for the servers endpoint. NLB endpoint could then be used to query metrics.
            "server": {
                "service": {
                    "type": "LoadBalancer"
                }
            },
            // Server configmap entries.
            //
            // This is copied from `deployment/artifacts/otel-deployment.yaml` to have the same footprint of
            // metrics being captured from prometheus.
            //
            // This overrides the configurations defined in the config map template in the chart.
            "serverFiles": {
                "prometheus.yml": prometheusScrapeConfigs,
            },
            "extraScrapeConfigs": [{
                "job_name": "kube-state-metrics",
                "metrics_path": "/metrics",
                "static_configs": [{
                    "targets": [input.apiServer],
                }],
            }]
        },
    }, {provider: k8sProvider})

    const output = pulumi.output({
        arn: "",  // ARN for a K8S pod does not exist
        prometheusWriteEndpoint: "",
        prometheusQueryEndpoint: "",
    })
    return output
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    if (input.useAMP) {
        return setupAMP(input)
    }
    return setupPrometheus(input)
}

async function run() {
    let output: pulumi.Output<outputType> | undefined;
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