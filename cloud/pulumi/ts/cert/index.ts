import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import { local } from "@pulumi/command";

import { getPrefix, fennelStdTags, Scope } from "../lib/util";
import { getProduct } from "@pulumi/aws/pricing";

export const plugins = {
    "kubernetes": "v3.18.0",
    "command": "v0.0.3",
    "aws": "v5.1.0",
}

export type inputType = {
    dnsName: string,
    kubeconfig: pulumi.Input<string>,
    namespace: string,
    scopeId: number,
    scope: Scope,
}

export type outputType = {
    tlsCertK8sSecretName: string
}

export const setup = async (input: inputType) => {
    const k8sProvider = new k8s.Provider("cert-k8s-provider", {
        kubeconfig: input.kubeconfig,
    })
    const prefix = getPrefix(input.scope, input.scopeId)
    // Create cert-manager namespace.
    const certManagerNamespace = new k8s.core.v1.Namespace(`${prefix}-cert-manager-ns`,
        {
            metadata: {
                name: "cert-manager",
                // See https://github.com/cert-manager/cert-manager/issues/4646 for disabling validation.
                annotations: {
                    "cert-manager.io/disable-validation": "true"
                },
            },
        }, { provider: k8sProvider }
    );
    // Deploy cert-manager resources.
    const certManager = new k8s.helm.v3.Chart(`${prefix}-cert-manager-res`, {
        fetchOpts: {
            "repo": "https://charts.jetstack.io"
        },
        chart: "cert-manager",
        version: "v1.9.1",
        namespace: "cert-manager",
        values: {
            installCRDs: true,
            global: {
                leaderElection: {
                    namespace: "cert-manager"
                }
            },
            // https://cert-manager.io/docs/configuration/acme/dns01/#setting-nameservers-for-dns01-self-check
            extraArgs: [
                "--dns01-recursive-nameservers-only",
                "--dns01-recursive-nameservers=8.8.8.8:53,1.1.1.1:53",
            ]
        }
    }, { provider: k8sProvider, dependsOn: certManagerNamespace })

    // Create cluster issuer.
    const clusterIssuer = new k8s.apiextensions.CustomResource(`${prefix}-cert-manager-cluster-issuer-res`, {
        "apiVersion": "cert-manager.io/v1",
        "kind": "ClusterIssuer",
        "metadata": {
            "name": "letsencrypt-prod",
        },
        "spec": {
            "acme": {
                "email": "amit@fennel.ai",
                "server": "https://acme-v02.api.letsencrypt.org/directory",
                "privateKeySecretRef": {
                    "name": "letsencrypt-prod"
                },
                "solvers": [{
                    "http01": {
                        "ingress": {
                            "class": "nginx"
                        },
                        "selector": "{}"
                    }

                }]
            }
        }
    }, { provider: k8sProvider, dependsOn: certManager })

    // Create Certificate.
    const certficate = new k8s.apiextensions.CustomResource(`${prefix}-certificate-res`, {
        "apiVersion": "cert-manager.io/v1",
        "kind": "Certificate",
        "metadata": {
            "name": "ambassador-certs",
            "namespace": input.namespace,
        },
        "spec": {
            "secretName": "ambassador-certs",
            "issuerRef": {
                "name": "letsencrypt-prod",
                "kind": "ClusterIssuer",
            },
            "dnsNames": [
                input.dnsName
            ]
        }
    }, { provider: k8sProvider, dependsOn: clusterIssuer })

    const acmeChallengMapping = new k8s.apiextensions.CustomResource(`${prefix}-acme-challenge-mapping`, {
        "apiVersion": "getambassador.io/v3alpha1",
        "kind": "Mapping",
        "metadata": {
            "name": "acme-challenge-mapping",
            "namespace": input.namespace,
        },
        "spec": {
            "hostname": "*",
            "prefix": " /.well-known/acme-challenge/",
            "rewrite": "",
            "service": "acme-challenge-service:2476"
        }
    }, { provider: k8sProvider, dependsOn: certficate, deleteBeforeReplace: true })

    const acmeChallengeService = new k8s.core.v1.Service(`${prefix}-acme-challenge-service`, {
        "metadata": {
            "name": "acme-challenge-service",
            "namespace": input.namespace,
        },
        "spec": {
            "ports": [{ port: 2476, targetPort: 8089 }],
            "selector": {
                "acme.cert-manager.io/http01-solver": "true"
            }
        }
    }, { provider: k8sProvider, dependsOn: certficate, deleteBeforeReplace: true })

    return {
        tlsCertK8sSecretName: "ambassador-certs",
    }
}
