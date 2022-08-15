import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v4.38.1",
    "kubernetes": "v3.18.0"
}

export type inputType = {
    useAMP: boolean,
    kubeconfig: pulumi.Input<any>,
    region: string,
    roleArn: pulumi.Input<string>,
    planeId: number,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

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
        "job_name": "kubernetes-nodes-cadvisor",
        "kubernetes_sd_configs": [{
            "role": "node"
        }],
        "scrape_interval": "10s",
        "scheme": "https",
        "metrics_path": "/metrics/cadvisor",
        "tls_config": {
            "ca_file": "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt",
            "insecure_skip_verify": true,
        },
        "bearer_token_file": "/var/run/secrets/kubernetes.io/serviceaccount/token",
        "relabel_configs": [{
            "action": "labelmap",
            "regex": "__meta_kubernetes_node_label_(.+)",
        }],
    }, {
        "job_name": "kubernetes-services",
        "kubernetes_sd_configs": [{
            "role": "service"
        }],
        "relabel_configs": [{
            "action": "labelmap",
            "regex": "__meta_kubernetes_service_label_(.+)"
        }, {
            "source_labels": ["__meta_kubernetes_namespace"],
            "target_label": "Namespace"
        }, {
            "source_labels": ["__meta_kubernetes_service_name"],
            "target_label": "Service"
        }],
    }],
}

async function setupAMP(input: inputType) {
    const awsProvider = new aws.Provider("prom-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const workspaceName = `p-${input.planeId}-prom`
    const prom = new aws.amp.Workspace(workspaceName, {
        alias: workspaceName,
    }, {provider: awsProvider, protect: input.protect});
}

async function setupPrometheus(input: inputType) {
    const k8sProvider = new k8s.Provider("prom-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })

    // By default prometheus chart creates 4 pods - alertmanager, node-exporter, pushgateway and server.
    // We disable alertmanager - since we will create alerts using grafana.
    // We disable node-exporter - node-exporter exports node and OS level metrics which are too granular for us now.
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
            // disable spinning up config map reloader
            //
            // this is not required as the helm release is updated altogether replacing the prometheus-server
            // while keeping the same PVC
            "configmapReload": {
                "prometheus": {
                    "enabled": false
                }
            },
            // Set service type as LoadBalancer so that AWS LBC creates a corresponding
            // NLB for the servers endpoint. NLB endpoint could then be used to query metrics.
            "server": {
                "service": {
                    "type": "LoadBalancer"
                },
                "nodeSelector": {
                    // we should schedule all components of Prometheus on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                },
                // https://github.com/prometheus-community/helm-charts/blob/main/charts/prometheus/values.yaml#L1124
                "retention": "60d",
                "extraFlags": [
                    // disable lock for the tsdb
                    //
                    // underneath the prometheus server captures a lock on the PVC. When the server is updated,
                    // it tries to grab a lock on the same PVC which results in a conflict and the container fails to
                    // come up. We have fixed this in the past by using `deleteBeforeReplace` but that resulted in
                    // deleted the PVC as well.
                    "storage.tsdb.no-lockfile"
                ]
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
            // enable scraping node labels to determine its capacity type and the node group it belongs to
            "kube-state-metrics": {
                // kube-state-metrics is enabled by default and is installed as a dependency
                metricLabelsAllowlist: ["nodes=[eks.amazonaws.com/capacityType,eks.amazonaws.com/nodegroup]"],
            }
        },
    }, {provider: k8sProvider, protect: input.protect});
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    await setupPrometheus(input);
    // prefer AMP's output over default prometheus since the endpoint of the AMP is required to export metrics to
    // it from otel deployment
    if (input.useAMP) {
        await setupAMP(input)
    }
    return pulumi.output({});
}
