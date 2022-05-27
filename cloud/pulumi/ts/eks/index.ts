import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes"
import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";
import * as aws from "@pulumi/aws";
import * as fs from 'fs';
import * as process from "process";
import * as path from 'path';

export const plugins = {
    "eks": "v0.37.1",
    "kubernetes": "v3.18.0",
    "command": "v0.0.3",
    "aws": "v4.38.1",
}

// NOTE: The AMI used should be an eks-worker AMI that can be searched
// on the AWS AMI catalog with one of the following prefixes:
// amazon-eks-node / amazon-eks-gpu-node / amazon-eks-arm64-node,
// depending on the type of machine provisioned.
const AMI_BY_REGION: Record<string, string> = {
    "ap-south-1": "ami-093fbc66b666c0da8",
    "us-west-2": "ami-0e1e876e558c727f4",
}

const DEFAULT_NODE_TYPE = "t3.medium"
const DEFAULT_DESIRED_CAPACITY = 3

export type inputType = {
    roleArn: string,
    region: string,
    vpcId: pulumi.Output<string>,
    connectedVpcCidrs: string[],
    planeId: number,
    nodeType?: string,
    desiredCapacity?: number,
}

export type outputType = {
    clusterName: string,
    kubeconfig: any,
    oidcUrl: string,
    instanceRole: string,
    workerSg: string,
    storageclasses: Record<string, string>
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
    const linkerd = new k8s.helm.v3.Release("linkerd", {
        repositoryOpts: {
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
    }, { provider: cluster })


    // Delete files
    new local.Command("cleanup", {
        create: "rm -f ca.crt ca.key issuer.crt issuer.key"
    }, { dependsOn: linkerd })

    return linkerd
}

async function setupEmissaryIngressCrds(input: inputType, awsProvider: aws.Provider, cluster: eks.Cluster) {
    // Setup AWS load balancer controller.
    const lbc = await setupLoadBalancerController(input, awsProvider, cluster)

    // Create CRDs.
    const root = process.env.FENNEL_ROOT!;
    const crdFile = path.join(root, "/deployment/artifacts/emissary-crds.yaml")
    const emissaryCrds = new k8s.yaml.ConfigFile("emissary-crds", {
        file: crdFile,
    }, { provider: cluster.provider, dependsOn: lbc })

    // Configure default Module to add linkerd headers as per:
    // https://www.getambassador.io/docs/edge-stack/latest/topics/using/mappings/#linkerd-interoperability-add_linkerd_headers

    // Wait for emissary-apiext deployment to be ready. There's no straightforward
    // way for us to ensure that the emissaryCrds config is applied before this
    // command is run, so we just wait for the deployment to be ready with a
    // large timeout (1 hr).
    const waiter = new local.Command("waiter", {
        create: "kubectl wait deploy/emissary-apiext --for condition=available -n emissary-system",
    }, { customTimeouts: { create: "1h" } })

    const l5dAmbConfig = waiter.stdout.apply(() => {
        return new k8s.apiextensions.CustomResource("l5d-amb-config", {
            "apiVersion": "getambassador.io/v3alpha1",
            "kind": "Module",
            "metadata": {
                "name": "ambassador"
            },
            "spec": {
                "config": {
                    "add_linkerd_headers": true,
                    // https://www.getambassador.io/docs/edge-stack/latest/topics/using/circuit-breakers/ and
                    //
                    // https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/circuit_breaking.html
                    //
                    // NOTE: circuit breaker configured below is a GLOBAL circuit breaker. If there are more than one endpoint
                    // configured in the future, consider making these limits at a `MAPPING` level.
                    "circuit_breakers": {
                        // Specifies the maximum number of connections that Ambassador Edge Stack will make to ALL hosts in the upstream cluster.
                        "max_connections": 3072,
                        // Specifies the maximum number of requests that will be queued while waiting for a connection.
                        "max_pending_requests": 1024,
                        // Specifies the maximum number of parallel outstanding requests to ALL hosts in a cluster at any given time.
                        "max_requests": 3072,
                        // default - "max_retries": 3,
                    }
                }
            }
        }, { provider: cluster.provider, dependsOn: waiter })
    })
}

async function setupIamRoleForServiceAccount(input: inputType, awsProvider: aws.Provider, namespace: string, serviceAccountName: string, cluster: eks.Cluster) {
    // Account id
    const current = await aws.getCallerIdentity({ provider: awsProvider });
    const accountId = current.accountId

    // Create k8s service account and IAM role for LoadBalancerController, and
    // associate the above policy with the account.
    const role = cluster.core.oidcProvider!.url.apply(oidcUrl => {
        return new aws.iam.Role(`p-${input.planeId}-lbc-role`, {
            namePrefix: `p-${input.planeId}-${serviceAccountName}`,
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

async function setupLoadBalancerController(input: inputType, awsProvider: aws.Provider, cluster: eks.Cluster) {
    const serviceAccountName = "aws-load-balancer-controller"

    // Create k8s service account and IAM role for LoadBalanacerController, and
    // associate the above policy with the account.
    const { role } = await setupIamRoleForServiceAccount(input, awsProvider, "kube-system", serviceAccountName, cluster)

    // Create policy for lb-controller.
    try {
        const root = process.env.FENNEL_ROOT!;
        const policyFilePath = path.join(root, "/deployment/artifacts/iam-policy.json")
        var policyJson = fs.readFileSync(policyFilePath, 'utf8')
    } catch (err) {
        console.error(err)
        process.exit()
    }
    const iamPolicy = new aws.iam.Policy(`p-${input.planeId}-lbc-policy`, {
        namePrefix: `p-${input.planeId}-AWSLoadBalancerControllerIAMPolicy`,
        policy: policyJson,
    }, { provider: awsProvider })

    const attachPolicy = new aws.iam.RolePolicyAttachment(`p-${input.planeId}-attach-lbc-policy`, {
        role: role.id,
        policyArn: iamPolicy.arn,
    }, { provider: awsProvider })

    const lbcValues = cluster.core.cluster.name.apply(clustername => {
        return {
            "clusterName": clustername,
            "serviceAccount": {
                "create": false,
                "name": serviceAccountName,
            }
        }
    })

    const lbc = new k8s.helm.v3.Release("aws-lbc", {
        repositoryOpts: {
            repo: "https://aws.github.io/eks-charts"
        },
        chart: "aws-load-balancer-controller",
        namespace: "kube-system",
        values: lbcValues,
    }, { provider: cluster.provider, dependsOn: attachPolicy })
    return lbc
}

// Setup https://github.com/kubernetes-sigs/descheduler/.
// Descheduler for Kubernetes is used to rebalance clusters by evicting pods
// that can potentially be scheduled on better nodes.
function setupDescheduler(cluster: eks.Cluster) {
    const descheduler = new k8s.helm.v3.Release("descheduler", {
        repositoryOpts: {
            repo: "https://kubernetes-sigs.github.io/descheduler/",
        },
        chart: "descheduler",
        namespace: "kube-system",
        values: {
            // Run descheduler every 30 minutes.
            "schedule": "*/30 * * * *"
        }
    }, { provider: cluster.provider })
}

function setupStorageClasses(cluster: eks.Cluster): Record<string, pulumi.Output<string>> {
    // Setup storage classes for EBS io1 volumes.
    const io1 = new k8s.storage.v1.StorageClass("ebs-io1-50ops", {
        allowVolumeExpansion: true,
        reclaimPolicy: "Delete",
        provisioner: "kubernetes.io/aws-ebs",
        volumeBindingMode: "WaitForFirstConsumer",
        parameters: {
            "type": "io1",
            "iopsPerGB": "50",
            "encrypted": "true",
            "fsType": "ext4",
        }
    }, { provider: cluster.provider })

    return { "io1": io1.metadata.name }
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const { vpcId, region, roleArn } = input

    const awsProvider = new aws.Provider("eks-aws-provider", {
        region: <aws.Region>region,
        assumeRole: {
            roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    const subnetIds = vpcId.apply(async vpcId => {
        return await aws.ec2.getSubnetIds({
            vpcId
        }, { provider: awsProvider })
    })

    const nodeCapacity = input.desiredCapacity || DEFAULT_DESIRED_CAPACITY

    // Create an EKS cluster with the default configuration.
    const cluster = new eks.Cluster(`p-${input.planeId}-eks-cluster`, {
        vpcId,
        endpointPrivateAccess: true,
        // TODO: disable public access once we figure out how to get the cluster
        // up and running when nodes are running in private subnet.
        endpointPublicAccess: true,
        subnetIds: subnetIds.ids,
        // TODO: Explore running different node groups, say one for countaggr service and one for http-service
        // with different instance types.
        nodeGroupOptions: {
            instanceType: input.nodeType || DEFAULT_NODE_TYPE,
            // NOTE: Desired number of nodes = 3. We have defined an affinity between the http-server and countaggr
            // services to NOT be scheduled on the same node (i.e. host).
            //
            // We currently run 2 replicas of http-server and 1 replica of countaggr
            //
            // This essentially means that one instance of each would reside on different nodes.
            //
            // Change number of replicas and/or affinity b/w services accordingly if updating the node group size.
            desiredCapacity: nodeCapacity,
            minSize: nodeCapacity,
            maxSize: nodeCapacity,
            // Make AMI a config parameter since AMI-ids are unique to region.
            amiId: AMI_BY_REGION[region],
            nodeAssociatePublicIpAddress: false,
        },
        // setup version for k8s control plane
        version: "1.22",
        providerCredentialOpts: {
            roleArn,
        },
        nodeAssociatePublicIpAddress: false,
        createOidcProvider: true
    }, { provider: awsProvider });

    // Install descheduler.
    setupDescheduler(cluster)

    // Connect cluster node security group to connected vpcs.
    const sgRules = new aws.ec2.SecurityGroupRule(`p-${input.planeId}-eks-sg-rule`, {
        type: "ingress",
        fromPort: 0,
        toPort: 65535,
        protocol: "tcp",
        cidrBlocks: input.connectedVpcCidrs,
        securityGroupId: cluster.nodeSecurityGroup.id
    }, { provider: awsProvider })

    const instanceRole = cluster.core.instanceRoles.apply((roles) => { return roles[0].name })

    const policy = new aws.iam.RolePolicy(`t-${input.planeId}-s3-createbucket-rolepolicy`, {
        name: `p-${input.planeId}-s3-createbucket-rolepolicy`,
        role: instanceRole,
        policy: `{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect":"Allow",
                    "Action": "s3:CreateBucket",
                    "Resource": "*"
                }
            ]
        }`,
    }, { provider: awsProvider });

    // Export the cluster's kubeconfig.
    const kubeconfig = cluster.kubeconfig;

    const oidcUrl = cluster.core.oidcProvider!.url

    // Setup linkerd service mesh.
    const linkerd = setupLinkerd(cluster.provider)

    // Install emissary-ingress CRDs after load-balancer controller.
    await setupEmissaryIngressCrds(input, awsProvider, cluster)

    // Setup fennel namespace.
    const ns = new k8s.core.v1.Namespace("fennel-ns", {
        metadata: {
            name: "fennel",
            annotations: {
                "linkerd.io/inject": "enabled",
            },
        }
    }, { provider: cluster.provider })

    // Setup storageclasses to be used by stateful sets.
    const storageclasses = setupStorageClasses(cluster)

    const workerSg = cluster.nodeSecurityGroup.id

    const clusterName = cluster.core.cluster.name

    const output = pulumi.output({
        kubeconfig, oidcUrl, instanceRole, workerSg, clusterName, storageclasses,
    })

    return output
}
