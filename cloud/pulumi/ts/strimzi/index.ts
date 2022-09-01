import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";

export const plugins = {
    "kubernetes": "v3.16.0"
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

    // install strimzi using helm charts
    const strimzi = new k8s.helm.v3.Release("strimzi", {
        repositoryOpts: {
            repo: "https://strimzi.io/charts/",
        },
        chart: "strimzi-kafka-operator",
        version: "0.30.0",
        namespace: ns.metadata.name,
        values: {},
    }, { provider: k8sProvider });

    return pulumi.output({})
}
