import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import process from "process";
import path from "path";

export const plugins = {
    "kubernetes": "v3.20.1"
}

export type inputType = {
    kubeconfig: pulumi.Input<any>,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {

    const k8sProvider = new k8s.Provider("strimzi-k8s-provider", {
        kubeconfig: input.kubeconfig,
    });

    const ns = new k8s.core.v1.Namespace("strimzi-ns", {
        metadata: {
            name: "strimzi",
        }
    }, { provider: k8sProvider });

    const root = process.env.FENNEL_ROOT!;
    const deploymentFilePath = path.join(root, "/cloud/pulumi/ts/strimzi/mirror-maker2.yaml")

    // register config map for metrics
    const metricsconfig = new k8s.yaml.ConfigFile("mirrormaker2-metrics", {
        file: deploymentFilePath,
    }, { provider: k8sProvider, replaceOnChanges: ["*"] });

    // install strimzi using helm charts
    const strimzi = new k8s.helm.v3.Release("strimzi", {
        repositoryOpts: {
            repo: "https://strimzi.io/charts/",
        },
        chart: "strimzi-kafka-operator",
        // we use kafka version 2.6.2 in msk cluster, so need to set the operator version to 0.23.0
        //
        // https://github.com/strimzi/strimzi-kafka-operator/releases/tag/0.23.0
        version: "0.23.0",
        namespace: ns.metadata.name,
        values: {
            nodeSelector: {
                "kubernetes.io/arch": "amd64",
            }
        },
    }, { provider: k8sProvider });

    return pulumi.output({})
}
