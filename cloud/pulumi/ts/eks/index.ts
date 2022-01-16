import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes"
import { local } from "@pulumi/command";

function setupLinkerd(cluster: k8s.Provider) {
    // Setup root and issuer CA as per https://linkerd.io/2.11/tasks/generate-certificates/.
    const rootCA = new local.Command("root-ca", {
        create: "step certificate create root.linkerd.cluster.local ca.crt ca.key --profile root-ca --no-password --insecure"
    })

    const readCaCrt = new local.Command("ca-crt", {
        create: "cat ca.crt"
    }, { dependsOn: rootCA })

    const issuerCA = new local.Command("issuer-ca", {
        create: "step certificate create identity.linkerd.cluster.local issuer.crt issuer.key --profile intermediate-ca --not-after 8760h --no-password --insecure --ca ca.crt --ca-key ca.key",
    }, { dependsOn: rootCA })

    const readIssuerCrt = new local.Command("issuer-crt", {
        create: "cat issuer.crt"
    }, { dependsOn: issuerCA })

    const readIssuerKey = new local.Command("issuer-key", {
        create: "cat issuer.key"
    }, { dependsOn: issuerCA })

    // Install linkerd
    const linkerd = new k8s.helm.v3.Chart("linkerd", {
        fetchOpts: {
            "repo": "https://helm.linkerd.io/stable"
        },
        chart: "linkerd2",
        version: "2.11",
        values: {
            "identityTrustAnchorsPEM": readCaCrt.stdout,
            "identity": {
                "issuer": {
                    "tls": {
                        "crtPEM": readIssuerCrt.stdout,
                        "keyPEM": readIssuerKey.stdout
                    }
                }
            }
        }
    }, { provider: cluster, dependsOn: [readCaCrt, readIssuerCrt, readIssuerKey] })


    // Delete files
    new local.Command("cleanup", {
        create: "rm -f ca.crt ca.key issuer.crt issuer.key"
    }, { dependsOn: linkerd.ready })
}


// Create an EKS cluster with the default configuration.
const cluster = new eks.Cluster("eks-cluster", {
    nodeGroupOptions: {
        desiredCapacity: 1,
        amiId: "ami-047a7967ea0436232"
    },
    providerCredentialOpts: {
        profileName: "admin"
    }
});

// Export the cluster's kubeconfig.
export const kubeconfig = cluster.kubeconfig;

// Setup linkerd service mesh.
setupLinkerd(cluster.provider)