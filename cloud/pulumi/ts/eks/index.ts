import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes"
import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";
import * as aws from "@pulumi/aws";
import * as fs from 'fs';
import * as process from "process";
import * as path from 'path';

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "eks": "v0.36.0",
    "kubernetes": "v3.15.0",
    "command": "v0.0.3",
    "aws": "v4.36.0",
}

// NOTE: The AMI used should be an eks-worker AMI that can be searched
// on the AWS AMI catalog with one of the following prefixes:
// amazon-eks-node / amazon-eks-gpu-node / amazon-eks-arm64-node,
// depending on the type of machine provisioned.
const AMI_BY_REGION: Record<string, string> = {
    "ap-south-1": "ami-018410e7cefe1d15f",
    "us-west-2": "ami-047a7967ea0436232",
}

export type inputType = {
    roleArn: string,
    region: string,
    vpcId: string,
    connectedVpcCidrs: string[],
}

export type outputType = {
    clusterName: pulumi.Output<string>,
    kubeconfig: pulumi.Output<any>,
    oidcUrl: pulumi.Output<string>,
    instanceRole: pulumi.Output<string>,
    workerSg: pulumi.Output<string>,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        roleArn: config.require(nameof<inputType>("roleArn")),
        region: config.require(nameof<inputType>("region")),
        vpcId: config.require(nameof<inputType>("vpcId")),
        connectedVpcCidrs: config.requireObject(nameof<inputType>("connectedVpcCidrs")),
    }
}

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

function setupEmissaryIngressCrds(cluster: k8s.Provider) {
    // Create CRDs.
    const root = process.env.FENNEL_ROOT!;
    const crdFile = path.join(root, "/deployment/artifacts/emissary-crds.yaml")
    const emissaryCrds = new k8s.yaml.ConfigFile("emissary-crds", {
        file: crdFile,
    }, { provider: cluster })


    // Configure default Module to add linkerd headers as per:
    // https://www.getambassador.io/docs/edge-stack/latest/topics/using/mappings/#linkerd-interoperability-add_linkerd_headers
    const l5dmapping = emissaryCrds.resources.apply(() => {
        return new k8s.apiextensions.CustomResource("l5d-mapping", {
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
        }, { provider: cluster })
    })
}

async function setupIamRoleForServiceAccount(awsProvider: aws.Provider, namespace: string, serviceAccountName: string, cluster: eks.Cluster) {
    // Account id
    const current = await aws.getCallerIdentity({ provider: awsProvider });
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
                     "${oidcUrl}:sub": "system:serviceaccount:${namespace}:${serviceAccountName}",
                     "${oidcUrl}:aud": "sts.amazonaws.com"
                   }
                 }
               }
             ]
           }`
        }, { provider: awsProvider })
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

async function setupLoadBalancerController(awsProvider: aws.Provider, cluster: eks.Cluster) {
    const serviceAccountName = "aws-load-balancer-controller"

    // Create k8s service account and IAM role for LoadBalanacerController, and
    // associate the above policy with the account.
    const { role } = await setupIamRoleForServiceAccount(awsProvider, "kube-system", serviceAccountName, cluster)

    // Create policy for lb-controller.
    try {
        const root = process.env.FENNEL_ROOT!;
        const policyFilePath = path.join(root, "/deployment/artifacts/iam-policy.json")
        var policyJson = fs.readFileSync(policyFilePath, 'utf8')
    } catch (err) {
        console.error(err)
        process.exit()
    }
    const iamPolicy = new aws.iam.Policy("lbc-policy", {
        namePrefix: "AWSLoadBalancerControllerIAMPolicy",
        policy: policyJson,
    }, { provider: awsProvider })

    const attachPolicy = new aws.iam.RolePolicyAttachment("attach-lbc-policy", {
        role: role.id,
        policyArn: iamPolicy.arn,
    }, { provider: awsProvider })

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


export const setup = async (input: inputType) => {
    const { vpcId, region, roleArn } = input

    const awsProvider = new aws.Provider("eks-aws-provider", {
        region: <aws.Region>region,
        assumeRole: {
            roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const subnetIds = await aws.ec2.getSubnetIds({
        vpcId
    }, { provider: awsProvider })

    // Create an EKS cluster with the default configuration.
    const cluster = new eks.Cluster("eks-cluster", {
        vpcId,
        endpointPrivateAccess: true,
        // TODO: disable public access once we figure out how to get the cluster
        // up and running when nodes are running in private subnet.
        endpointPublicAccess: true,
        subnetIds: subnetIds.ids,
        nodeGroupOptions: {
            instanceType: "t2.medium",
            desiredCapacity: 3,
            minSize: 3,
            maxSize: 3,
            // Make AMI a config parameter since AMI-ids are unique to region.
            amiId: AMI_BY_REGION[region],
        },
        providerCredentialOpts: {
            roleArn,
        },
        nodeAssociatePublicIpAddress: false,
        createOidcProvider: true
    }, { provider: awsProvider });

    // Connect cluster node security group to connected vpcs.
    const sgRules = new aws.ec2.SecurityGroupRule(`eks-sg-rule`, {
        type: "ingress",
        fromPort: 0,
        toPort: 65535,
        protocol: "tcp",
        cidrBlocks: input.connectedVpcCidrs,
        securityGroupId: cluster.nodeSecurityGroup.id
    }, { provider: awsProvider })

    const instanceRole = cluster.core.instanceRoles.apply((roles) => { return roles[0].name })

    // Export the cluster's kubeconfig.
    const kubeconfig = cluster.kubeconfig;

    const oidcUrl = cluster.core.oidcProvider!.url

    // Setup linkerd service mesh.
    setupLinkerd(cluster.provider)

    // Setup AWS load balancer controller.
    const lbc = await setupLoadBalancerController(awsProvider, cluster)

    // Install emissary-ingress CRDs after load-balancer controller.
    lbc.ready.apply(() => {
        return setupEmissaryIngressCrds(cluster.provider)
    })

    // Setup fennel namespace.
    const ns = new k8s.core.v1.Namespace("fennel-ns", {
        metadata: {
            name: "fennel",
            annotations: {
                "linkerd.io/inject": "enabled",
            },
        }
    }, { provider: cluster.provider })

    const workerSg = cluster.nodeSecurityGroup.id

    const clusterName = cluster.core.cluster.name

    const output: outputType = {
        kubeconfig, oidcUrl, instanceRole, workerSg, clusterName
    }

    return output
}

async function run() {
    let output: outputType | undefined;
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
