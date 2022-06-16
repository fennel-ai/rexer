import * as eks from "@pulumi/eks";
import * as k8s from "@pulumi/kubernetes"
import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";
import * as aws from "@pulumi/aws";
import * as fs from 'fs';
import * as process from "process";
import * as path from 'path';
import {provider} from "@pulumi/pulumi";

export const plugins = {
    "eks": "0.39.0",
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
const DEFAULT_MIN_SIZE = 1
const DEFAULT_MAX_SIZE = 3

// Node Group configuration for the EKS cluster
export type NodeGroupConf = {
    // Must be unique across node groups defined in the same plane
    name: string,
    nodeType: string,
    // take the following into consideration before setting this value:
    //  i) pods and services (and their replicas) which will run on this node group
    //  ii) availability of the services - if there more than one public facing service, it might be better to have more
    //      than 1 node to have better fault-tolerance
    //  iii) for a prod plane, consider setting this to >= 2 to avoid tainted node problems
    minSize: number,
    // this is the maximum size of the node group up-to which it can be scaled up to.
    //
    // NOTE: Take quota limits into consideration before setting this value -
    //  https://docs.aws.amazon.com/eks/latest/userguide/service-quotas.html
    maxSize: number,
    // labels to be attached to the node group
    labels?: Record<string, string>,
}

export type inputType = {
    roleArn: pulumi.Input<string>,
    region: string,
    vpcId: pulumi.Output<string>,
    connectedVpcCidrs: string[],
    publicSubnets: pulumi.Output<string[]>,
    privateSubnets: pulumi.Output<string[]>,
    planeId: number,
    nodeGroups?: NodeGroupConf[],
}

export type outputType = {
    clusterName: string,
    kubeconfig: any,
    oidcUrl: string,
    instanceRole: string,
    clusterSg: string,
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

async function setupClusterAutoscaler(awsProvider: aws.Provider, input: inputType, cluster: eks.Cluster) {
    // Account ID
    const current = await aws.getCallerIdentity({ provider: awsProvider });
    const accountId = current.accountId;

    // See: https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html
    const roleName = `p-${input.planeId}-autoscaler-role`;

    const role = pulumi.all([cluster.core.oidcProvider!.url, cluster.core.cluster.name]).apply(([oidcUrl, clusterName]) => {
        return new aws.iam.Role(roleName, {
            namePrefix: roleName,
            description: "IAM role for EKS cluster autoscaler",
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
                         "${oidcUrl}:sub": "system:serviceaccount:kube-system:cluster-autoscaler"
                       }
                     }
                   }
                 ]
               }`,
            inlinePolicies: [{
                name: "eks-cluster-autoscaler-policy",
                policy: `{
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": [
                                "autoscaling:SetDesiredCapacity",
                                "autoscaling:TerminateInstanceInAutoScalingGroup"
                            ],
                            "Resource": "*",
                            "Condition": {
                                "StringEquals": {
                                    "aws:ResourceTag/k8s.io/cluster-autoscaler/${clusterName}": "owned"
                                }
                            }
                        },
                        {
                            "Effect": "Allow",
                            "Action": [
                                "autoscaling:DescribeAutoScalingGroups",
                                "autoscaling:DescribeAutoScalingInstances",
                                "autoscaling:DescribeInstances",
                                "autoscaling:DescribeLaunchConfigurations",
                                "autoscaling:DescribeTags",
                                "ec2:DescribeLaunchTemplateVersions",
                                "ec2:DescribeInstanceTypes"
                            ],
                            "Resource": "*"
                        }
                    ]
                }`,
            }],
        }, { provider: awsProvider });
    });

    // Setup the cluster autoscaler
    //
    // Currently the cluster autoscaler ensures that none of the pods are un-schedulable.
    //
    // NOTE: our setup with node-selectors and affinity does not allow cluster autoscaler to run at it's full power.
    // This is currently setup along with Horizontal Pod Autoscaler which increases/decreases the pods, which could
    // require adding/removing a new node, which is actuated by the cluster autoscaler.
    return pulumi.all([role.arn, cluster.core.cluster.name]).apply(([roleArn, clusterName]) => {
        const autoscalerName = `p-${input.planeId}-cluster-autoscaler`;
        return new k8s.helm.v3.Release(autoscalerName, {
            repositoryOpts: {
                "repo": "https://kubernetes.github.io/autoscaler",
            },
            // this must match the namespace provided in the role above.
            namespace: "kube-system",
            chart: "cluster-autoscaler",
            values: {
                // auto-discover the autoscaling groups of the EKS cluster (since we use managed node groups, the necessary
                // tags (`k8s.io/cluster-autoscaler/enabled` and `k8s.io/cluster-autoscaler/<CLUSTER_NAME>`) are
                // already applied.
                "autoDiscovery": {
                    "clusterName": clusterName,
                },
                "awsRegion": input.region,
                "cloudProvider": "aws",
                // create 2 replicas for the cluster autoscaler to be fault-tolerant.
                "replicaCount": 1,
                // autoscaler exports prometheus metrics, enable scraping them through our telemetry setup
                "podAnnotations": {
                    "prometheus.io/scrape": "true",
                    // the port is the default value for the port of the service
                    "prometheus.io/port": "8085",
                },
                // annotate the service account with the IAM role
                "rbac": {
                    "serviceAccount": {
                        // this must match the name provided above in the role.
                        "name": "cluster-autoscaler",
                        "annotations": {
                            "eks.amazonaws.com/role-arn": roleArn,
                        }
                    }
                },
                // override the full name as the one created by the helm release is long and has redundant words
                "fullnameOverride": autoscalerName,
                // "extraArgs" needs to be set to tune the autoscaler as per:
                // https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-are-the-parameters-to-ca
            }
        }, { provider: cluster.provider, deleteBeforeReplace: true });
    });
}

async function setupMetricsServer(provider: aws.Provider, input: inputType, cluster: eks.Cluster) {
    const metricServerName = `p-${input.planeId}-metrics-server`
    return new k8s.helm.v3.Release(metricServerName, {
        repositoryOpts: {
            repo: "https://kubernetes-sigs.github.io/metrics-server/"
        },
        chart: "metrics-server",
        namespace: "kube-system",
        values: {
            "fullnameOverride": metricServerName,
        }
    }, { provider: cluster.provider, deleteBeforeReplace: true })
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const { vpcId, publicSubnets, privateSubnets, region, roleArn } = input

    const awsProvider = new aws.Provider("eks-aws-provider", {
        region: <aws.Region>region,
        assumeRole: {
            roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create an EKS cluster with the default configuration.
    const cluster = new eks.Cluster(`p-${input.planeId}-eks-cluster`, {
        vpcId,
        endpointPrivateAccess: true,
        // TODO: disable public access once we figure out how to get the cluster
        // up and running when nodes are running in private subnet.
        endpointPublicAccess: true,
        publicSubnetIds: publicSubnets,
        privateSubnetIds: privateSubnets,
        // setup version for k8s control plane
        version: "1.22",
        providerCredentialOpts: {
            roleArn,
        },
        // Make AMI a config parameter since AMI-ids are unique to region.
        nodeAmiId: AMI_BY_REGION[region],
        nodeAssociatePublicIpAddress: false,
        createOidcProvider: true,
        // Skip creating default node group since we explicitly create a managed node group (default one even if
        // not specified in the configuration).
        skipDefaultNodeGroup: true,
        // Enable EKS control plane logging - this will start sending logs to cloudwatch.
        //
        // Allow values - ["api", "audit", "authenticator", "controllerManager", "scheduler"]
        // See: https://docs.aws.amazon.com/eks/latest/userguide/control-plane-logs.html
        enabledClusterLogTypes: ["api", "authenticator", "controllerManager", "scheduler"],
    }, { provider: awsProvider });

    // setup cluster autoscaler
    const autoscaler = setupClusterAutoscaler(awsProvider, input, cluster);

    // setup metrics server for autoscaling needs
    const metricsServer = setupMetricsServer(awsProvider, input, cluster);

    // Get the cluster security group created by EKS for managed node groups and fargate.
    // Source: https://docs.aws.amazon.com/eks/latest/userguide/sec-group-reqs.html
    const clusterSg = cluster.eksCluster.vpcConfig.clusterSecurityGroupId;

    const instanceRole = cluster.core.instanceRoles.apply((roles) => { return roles[0].name })
    const instanceRoleArn = cluster.core.instanceRoles.apply((roles) => { return roles[0].arn })

    const defaultNodeGroup = {
        name: `p-${input.planeId}-default-nodegroup`,
        nodeType: DEFAULT_NODE_TYPE,
        minSize: DEFAULT_MIN_SIZE,
        maxSize: DEFAULT_MAX_SIZE,
    };
    let nodeGroups: NodeGroupConf[] = input.nodeGroups !== undefined ? input.nodeGroups : [defaultNodeGroup];

    // Setup managed node groups
    for (let nodeGroup of nodeGroups) {
        const n = new eks.ManagedNodeGroup(nodeGroup.name, {
            cluster: cluster,
            scalingConfig: {
                // start the desired size with the min required size and let the cluster autoscaler
                // scale up the nodes up-to `maxSize` based on the schedulability of the pods.
                desiredSize: nodeGroup.minSize,
                minSize: nodeGroup.minSize,
                maxSize: nodeGroup.maxSize,
            },
            // accepts multiple strings but the EKS API accepts only a single string
            instanceTypes: [nodeGroup.nodeType],
            nodeGroupNamePrefix: nodeGroup.name,
            labels: nodeGroup.labels,
            nodeRoleArn: instanceRoleArn,
            subnetIds: privateSubnets,
        }, { provider: awsProvider });
    }

    // Install descheduler.
    setupDescheduler(cluster);

    // Connect cluster node security group to connected vpcs.
    const sgRules = new aws.ec2.SecurityGroupRule(`p-${input.planeId}-eks-sg-rule`, {
        type: "ingress",
        fromPort: 0,
        toPort: 65535,
        protocol: "tcp",
        cidrBlocks: input.connectedVpcCidrs,
        securityGroupId: clusterSg,
    }, { provider: awsProvider })

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
    const clusterName = cluster.core.cluster.name

    const output = pulumi.output({
        kubeconfig, oidcUrl, instanceRole, clusterSg, clusterName, storageclasses,
    })

    return output
}
