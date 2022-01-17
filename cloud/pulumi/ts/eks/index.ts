import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes"
import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";

function setupLinkerd(cluster: k8s.Provider) {
    // Setup root and issuer CA as per https://linkerd.io/2.11/tasks/generate-certificates/.
    const rootCA = new local.Command("root-ca", {
        create: "step certificate create root.linkerd.cluster.local ca.crt ca.key --profile root-ca --no-password --insecure",
        delete: "rm -f ca.crt ca.key"
    })

    const readCaCrt = new local.Command("ca-crt", {
        create: "cat ca.crt"
    }, { dependsOn: rootCA })

    const issuerCA = new local.Command("issuer-ca", {
        create: "step certificate create identity.linkerd.cluster.local issuer.crt issuer.key --profile intermediate-ca --not-after 8760h --no-password --insecure --ca ca.crt --ca-key ca.key",
        delete: "rm -f issuer.crt issuer.key"
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

function setupAmbassadorIngress(cluster: k8s.Provider) {

    // Create namespace.
    const ns = new k8s.core.v1.Namespace("aes-ns", {
        metadata: {
            name: "ambassador"
        }
    })

    // Create CRDs.
    const aesCrds = new k8s.yaml.ConfigFile("aes-cerds", {
        file: "aes-crds.yaml"
    }, { provider: cluster, dependsOn: ns })

    // Configure default Module to add linkerd headers as per:
    // https://www.getambassador.io/docs/edge-stack/latest/topics/using/mappings/#linkerd-interoperability-add_linkerd_headers
    const l5dmapping = new k8s.apiextensions.CustomResource("l5d-mapping", {
        "apiVersion": "getambassador.io/v3alpha1",
        "kind": "Module",
        "metadata": {
            "name": "ambassador"
        },
        "spec": {
            "config": {
                "add_linkerd_headers": true
            }
        }
    }, { provider: cluster, dependsOn: aesCrds })

    const config = new pulumi.Config();

    // Install ambassador via helm.
    const ambassador = new k8s.helm.v3.Chart("aes", {
        fetchOpts: {
            repo: "https://app.getambassador.io"
        },
        chart: "edge-stack",
        version: "7.2.1",
        namespace: ns.id,
        values: {
            "emissary-ingress": {
                "createDefaultListeners": true,
                "agent": {
                    // Token to connect cluster to ambassador cloud.
                    "cloudConnectToken": config.requireSecret("aes-token")
                }
            }
        },
        transformations: [
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Deployment" && obj.metadata.name === "aes-edge-stack") {
                    const metadata = obj.spec.template.metadata
                    if (metadata.annotations == null) {
                        metadata.annotations = {}
                    }
                    // We use inject=enabled instead of inject=ingress as per
                    // https://github.com/linkerd/linkerd2/issues/6650#issuecomment-898732177.
                    // Otherwise, we see the issue reported in the above bug report.
                    metadata.annotations["linkerd.io/inject"] = "enabled"
                    metadata.annotations["config.linkerd.io/skip-inbound-ports"] = "80,443"
                }
            },
        ]
    }, { provider: cluster, dependsOn: [aesCrds, l5dmapping] })
}


// Create an EKS cluster with the default configuration.
const cluster = new eks.Cluster("eks-cluster", {
    nodeGroupOptions: {
        desiredCapacity: 3,
        minSize: 3,
        maxSize: 3,
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


// Install Ambassador
setupAmbassadorIngress(cluster.provider)