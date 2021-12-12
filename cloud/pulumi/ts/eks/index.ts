import * as eks from "@pulumi/eks";

// Create an EKS cluster with the default configuration.
const cluster = new eks.Cluster("eks-cluster", {
    nodeGroupOptions: {
        desiredCapacity: 1,
    },
    providerCredentialOpts: {
        "profileName": "admin"
    }
});

// Export the cluster's kubeconfig.
export const kubeconfig = cluster.kubeconfig;