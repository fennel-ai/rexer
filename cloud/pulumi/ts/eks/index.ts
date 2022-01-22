import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes"
import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";
import * as aws from "@pulumi/aws";
import * as fs from 'fs';
import { CustomResource } from "@pulumi/pulumi";

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

function setupAmbassadorIngress(cluster: k8s.Provider): pulumi.Output<string> {
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
    }, { provider: cluster, dependsOn: aesCrds.ready })

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
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Service" && obj.spec.type === "LoadBalancer") {
                    const metadata = obj.metadata
                    if (metadata.annotations == null) {
                        metadata.annotations = {}
                    }
                    // Set load-balancer type as external to bypass k8s in-tree
                    // load-balancer controller and use AWS Load Balancer Controller
                    // instead.
                    // https://kubernetes-sigs.github.io/aws-load-balancer-controller/v2.3/guide/service/nlb/#configuration
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] = "external"
                    // Use NLB in instance mode since we don't currently setup the VPC CNI plugin.
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-nlb-target-type"] = "instance"
                    // Deploy an internal load-balancer instead of an internet-facing one.
                    metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-scheme"] = "internal"
                }
            },
        ]
    }, { provider: cluster, dependsOn: l5dmapping })

    return ambassador.ready.apply((_) => {
        const ingressResource = ambassador.getResource("v1/Service", "ambassador", "aes-edge-stack");
        return ingressResource.status.loadBalancer.ingress[0].hostname
    })
}

async function setupIamRoleForServiceAccount(namespace: string, serviceAccountName: string, cluster: eks.Cluster) {
    // Account id
    const current = await aws.getCallerIdentity({});
    const accountId = current.accountId

    // Create k8s service account and IAM role for LoadBalanacerController, and
    // associate the above policy with the account.
    const role = cluster.core.oidcProvider!.url.apply(oidcUrl => {
        return new aws.iam.Role("lbc-role", {
            namePrefix: serviceAccountName,
            description: "IAM role for AWS load-balancer-controller",
            assumeRolePolicy: `{
             "Version": "2012-10-17",
             "Statement": [
               {
                 "Effect": "Allow",
                 "Principal": {
                   "Federated": "arn:aws:iam::${accountId}:oidc-provider/${oidcUrl}"
                 },
                 "Action": "sts:AssumeRoleWithWebIdentity",
                 "Condition": {
                   "StringEquals": {
                     "${oidcUrl}:sub": "system:serviceaccount:${namespace}:${serviceAccountName}"
                   }
                 }
               }
             ]
           }`
        })
    })

    const acc = role.apply(role => {
        new k8s.core.v1.ServiceAccount("lbc-ac", {
            automountServiceAccountToken: true,
            metadata: {
                name: serviceAccountName,
                namespace: namespace,
                annotations: {
                    "eks.amazonaws.com/role-arn": role.arn
                }
            }
        }, { provider: cluster.provider })
    })

    return { "role": role, "serviceAccount": acc }
}

async function setupLoadBalancerController(cluster: eks.Cluster) {

    const serviceAccountName = "aws-load-balancer-controller"

    // Create k8s service account and IAM role for LoadBalanacerController, and
    // associate the above policy with the account.
    const { role } = await setupIamRoleForServiceAccount("kube-system", serviceAccountName, cluster)

    // Create policy for lb-controller.
    try {
        var policyJson = fs.readFileSync('iam-policy.json', 'utf8')
    } catch (err) {
        console.error(err)
        process.exit()
    }
    const iamPolicy = new aws.iam.Policy("lbc-policy", {
        namePrefix: "AWSLoadBalancerControllerIAMPolicy",
        policy: policyJson,
    })

    const attachPolicy = new aws.iam.RolePolicyAttachment("attach-lbc-policy", {
        role: role.id,
        policyArn: iamPolicy.arn,
    })

    const lbc = new k8s.helm.v3.Chart("aws-lbc", {
        fetchOpts: {
            repo: "https://aws.github.io/eks-charts"
        },
        chart: "aws-load-balancer-controller",
        namespace: "kube-system",
        values: {
            "clusterName": cluster.core.cluster.name,
            "serviceAccount": {
                "create": false,
                "name": serviceAccountName,
            }
        }
    }, { provider: cluster.provider, dependsOn: attachPolicy })

    return lbc
}

export = async () => {
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
        },
        createOidcProvider: true
    });

    // Export the cluster's kubeconfig.
    const kubeconfig = cluster.kubeconfig;

    const oidcUrl = cluster.core.oidcProvider?.url

    // Setup linkerd service mesh.
    setupLinkerd(cluster.provider)

    // Setup AWS load balancer controller.
    const lbc = await setupLoadBalancerController(cluster)

    // Install Ambassador after load-balancer controller.
    const ingress = lbc.ready.apply((_) => {
        return setupAmbassadorIngress(cluster.provider)
    })

    return { kubeconfig, oidcUrl, ingress }
}