import * as eks from "@pulumi/eks";
import * as docker from "@pulumi/docker";
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";
import * as aws from "@pulumi/aws";
import * as fs from 'fs';
import * as process from "process";
import * as path from 'path';
import { exit } from "process";
import { getPrefix, Scope } from "../lib/util"

// NOTE: The AMI used should be an eks-worker AMI that can be searched
// on the AWS AMI catalog with one of the following prefixes:
// amazon-eks-node / amazon-eks-gpu-node / amazon-eks-arm64-node,
// depending on the type of machine provisioned.
export const DEFAULT_X86_AMI_TYPE = "AL2_x86_64"
export const DEFAULT_ARM_AMI_TYPE = "AL2_ARM_64"

export const SPOT_INSTANCE_TYPE = "SPOT";
export const ON_DEMAND_INSTANCE_TYPE = "ON_DEMAND";

export const plugins = {
    "eks": "0.39.0",
    "kubernetes": "v3.20.1",
    "command": "v0.0.3",
    "aws": "v5.0.0",
}

export type SpotReschedulerConf = {
    // Label attached to the spot nodes, on which the on-demand node's workload can be migrated to
    spotNodeLabel: string,
    // Label attached to the on-demand node, from which the workload will be migrated to spot nodes
    onDemandNodeLabel: string,
}

// Node Group configuration for the EKS cluster
export type NodeGroupConf = {
    // Must be unique across node groups defined in the same plane
    name: string,
    // list of instance types in this node group
    //
    // NOTE: Ideally these instance types should have identical resource specs (e.g. CPU, Memory etc). Kubernetes
    // cluster autoscaler (which is configured for our EKS cluster) does not behave well when a node group has
    // multiple instance types with different resource specs
    // see - https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/cloudprovider/aws/README.md#using-mixed-instances-policies-and-spot-instances
    instanceTypes: string[],
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
    amiType: string,
    // Type of the instance to use in this node group
    capacityType: string,
    // labels to be attached to the node group
    labels?: Record<string, string>,
    // priority assigned to the autoscaling group backing this node group for the Cluster Autoscaler to select
    // for expansion in case a new node needs to be scheduled for scheduling a pod
    //
    // This has the following requirements and expansion behavior -
    //  i. Must be a positive value
    //  ii. The highest value is given the priority i.e. highest value wins
    //  iii. If multiple node groups have the same priority, a node group among them is selected at random
    expansionPriority: number,
}

export type inputType = {
    roleArn: pulumi.Input<string>,
    region: string,
    vpcId: pulumi.Output<string>,
    connectedVpcCidrs?: string[],
    publicSubnets: pulumi.Output<string[]>,
    privateSubnets: pulumi.Output<string[]>,
    planeId: number,
    nodeGroups: NodeGroupConf[],
    spotReschedulerConf?: SpotReschedulerConf,
    scope: Scope,
}

export type outputType = {
    clusterName: string,
    kubeconfig: any,
    oidcUrl: string,
    instanceRole: string,
    instanceRoleArn: string,
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
            },
            "nodeSelector": {
                "kubernetes.io/os": "linux",
                // we should schedule all components of Linkerd control plane on ON_DEMAND instances
                "eks.amazonaws.com/capacityType": "ON_DEMAND",
            },
            "proxy": {
                // Add all the ports specified in the default value + our HTTP/Query server port
                //
                // Default set of opaque ports:
                // - SMTP (25,587) server-first
                // - MYSQL (3306) server-first
                // - Galera (4444) server-first
                // - PostgreSQL (5432) server-first
                // - Redis (6379) server-first
                // - ElasticSearch (9300) server-first
                // - Memcached (11211)
                // clients do not issue any preamble, which breaks detection
                //
                // - HTTP/Query server 2425
                //
                // NOTE: it seems like these ports are marked opaque only if it is a container port on that pod
                // and the rest are ignored
                //
                // TODO(mohit): Migrate away from setting HTTP/Query server port here, instead this should be
                // configurable on a pod level. Awaiting a fix/response in - https://github.com/linkerd/linkerd2/issues/8922
                "opaquePorts": "25,587,3306,4444,5432,6379,9300,11211,2425",
            }
        }
    }, { provider: cluster })


    // Delete files
    new local.Command("cleanup", {
        create: "rm -f ca.crt ca.key issuer.crt issuer.key"
    }, { dependsOn: linkerd })

    return linkerd
}

async function setupEKSLocalSSDProvisioner(cluster: eks.Cluster, awsProvider: aws.Provider) {
    const root = path.join(process.env.FENNEL_ROOT!, "deployment/artifacts/eks-nvme-ssd-provisioner");
    // Create eks-nvme-ssd-provisioner. This is responsible found mounting
    // attached disks.

    const repo = new aws.ecr.Repository(`nvme-ssd-provisioner-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    const registryInfo = repo.registryId.apply(async id => {
        const credentials = await aws.ecr.getCredentials({ registryId: id }, { provider: awsProvider });
        const decodedCredentials = Buffer.from(credentials.authorizationToken, "base64").toString();
        const [username, password] = decodedCredentials.split(":");
        if (!password || !username) {
            throw new Error("Invalid credentials");
        }
        return {
            server: credentials.proxyEndpoint,
            username: username,
            password: password,
        };
    });
    const imageName = repo.repositoryUrl.apply(imgName => {
        return `${imgName}:latest`
    });
    const dockerfile = path.join(root, "Dockerfile.alpine");
    const image = new docker.Image("nvme-ssd-provisioner-img", {
        build: {
            context: root,
            dockerfile: dockerfile,
            args: {
                // TODO: consider using docker buildx here
                "platform": "linux/arm64",
            },
        },
        imageName: imageName,
        registry: registryInfo,
    });

    const ssdProvisioner = image.imageName.apply(imgName => {
        return new k8s.yaml.ConfigFile(`eks-nvme-ssd-provisioner`, {
            file: path.join(root, "eks-nvme-ssd-provisioner.yaml"),
            transformations: [
                (obj: any, opts: pulumi.CustomResourceOptions) => {
                    if (obj.kind === "DaemonSet") {
                        obj.spec.template.spec.containers[0].image = imgName;
                    }
                },
            ]
        }, { provider: cluster.provider });
    });

    // Create resources for local-static-provisioner:
    // https://github.com/kubernetes-sigs/sig-storage-local-static-provisioner/
    const localStaticProvisioner = new k8s.yaml.ConfigFile("local-static-provisioner", {
        file: path.join(root, "storage-local-static-provisioner.yaml"),
    }, { provider: cluster.provider })
}

async function setupEmissaryIngressCrds(input: inputType, awsProvider: aws.Provider, cluster: eks.Cluster) {
    // Setup AWS load balancer controller.
    const lbc = await setupLoadBalancerController(input, awsProvider, cluster)

    // Create CRDs.
    const root = process.env.FENNEL_ROOT!;
    const crdFile = path.join(root, "/deployment/artifacts/emissary-crds.yaml")
    const emissaryCrds = new k8s.yaml.ConfigFile("emissary-crds", {
        file: crdFile,
        transformations: [
            (obj: any, opts: pulumi.CustomResourceOptions) => {
                if (obj.kind === "Deployment" && obj.metadata.name === `emissary-apiext`) {
                    obj.spec.template.spec.nodeSelector = {
                        "kubernetes.io/arch": "amd64",
                    }
                }
            },
        ]

    }, { provider: cluster.provider, dependsOn: lbc })

    // Configure default Module to add linkerd headers as per:
    // https://www.getambassador.io/docs/edge-stack/latest/topics/using/mappings/#linkerd-interoperability-add_linkerd_headers

    // Wait for emissary-apiext deployment to be ready. There's no straightforward
    // way for us to ensure that the emissaryCrds config is applied before this
    // command is run, so we just wait for the deployment to be ready with a
    // large timeout (1 hr).
    const waiter = new local.Command("waiter", {
        create: "kubectl wait deploy/emissary-apiext --for condition=available -n emissary-system",
    }, { customTimeouts: { create: "1h" }, dependsOn: emissaryCrds });
}

async function setupIamRoleForServiceAccount(input: inputType, awsProvider: aws.Provider, entity: string, namespace: string, serviceAccountName: string, cluster: eks.Cluster) {
    // Account id
    const current = await aws.getCallerIdentity({ provider: awsProvider });
    const accountId = current.accountId

    // Create IAM role and k8s service account.
    const role = cluster.core.oidcProvider!.url.apply(oidcUrl => {
        return new aws.iam.Role(`${getPrefix(input.scope, input.planeId)}-${entity}-role`, {
            namePrefix: `${getPrefix(input.scope, input.planeId)}-${serviceAccountName}`,
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
        new k8s.core.v1.ServiceAccount(`${entity}-ac`, {
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
    const { role } = await setupIamRoleForServiceAccount(input, awsProvider, "lbc", "kube-system", serviceAccountName, cluster)

    // Create policy for lb-controller.
    try {
        const root = process.env.FENNEL_ROOT!;
        const policyFilePath = path.join(root, "/deployment/artifacts/iam-policy.json")
        var policyJson = fs.readFileSync(policyFilePath, 'utf8')
    } catch (err) {
        console.error(err)
        process.exit()
    }
    const iamPolicy = new aws.iam.Policy(`${getPrefix(input.scope, input.planeId)}-lbc-policy`, {
        namePrefix: `${getPrefix(input.scope, input.planeId)}-AWSLoadBalancerControllerIAMPolicy`,
        policy: policyJson,
    }, { provider: awsProvider })

    const attachPolicy = new aws.iam.RolePolicyAttachment(`${getPrefix(input.scope, input.planeId)}-attach-lbc-policy`, {
        role: role.id,
        policyArn: iamPolicy.arn,
    }, { provider: awsProvider })

    const lbcValues = cluster.core.cluster.name.apply(clustername => {
        return {
            "clusterName": clustername,
            "serviceAccount": {
                "create": false,
                "name": serviceAccountName,
            },
            "nodeSelector": {
                // we should schedule all components of AWS LBC on ON_DEMAND instances
                "eks.amazonaws.com/capacityType": "ON_DEMAND",
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
            "schedule": "*/30 * * * *",
            "nodeSelector": {
                // we should schedule all components of K8S descheduler on ON_DEMAND instances
                "eks.amazonaws.com/capacityType": "ON_DEMAND",
            }
        }
    }, { provider: cluster.provider })
}

function setupStorageClasses(cluster: eks.Cluster): Record<string, pulumi.Output<string>> {
    // create a provider which enables server-side apply, this will be the default behavior in the upcoming releases
    //
    // https://www.pulumi.com/registry/packages/kubernetes/how-to-guides/managing-resources-with-server-side-apply/
    const ssaProvider = new k8s.Provider("ssa-storageclass-provider", {
        kubeconfig: cluster.kubeconfig,
        enableServerSideApply: true,
    });
    // patch default storage class to allow expansion
    const storageClassPatch = new k8s.storage.v1.StorageClassPatch("ebs-default-storage-class", {
        allowVolumeExpansion: true,
        metadata: {
            // EKS by default creates a storage class named gp2, of type gp2
            name: "gp2",
        }
    }, { provider: ssaProvider });

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

    const io2 = new k8s.storage.v1.StorageClass("ebs-io2-200ops", {
        allowVolumeExpansion: true,
        reclaimPolicy: "Delete",
        provisioner: "ebs.csi.aws.com",
        volumeBindingMode: "WaitForFirstConsumer",
        parameters: {
            "type": "io2",
            "iopsPerGB": "200",
            "encrypted": "true",
            "fsType": "ext4",
        }
    }, { provider: cluster.provider })

    return {
        "io1": io1.metadata.name,
        "io2": io2.metadata.name,
    }
}

// This function follows the EBS CSI driver's setup instructions from:
// https://github.com/kubernetes-sigs/aws-ebs-csi-driver/blob/master/docs/install.md
async function setupEbsCsiDriver(input: inputType, awsProvider: aws.Provider, cluster: eks.Cluster) {
    // Give the driver IAM permission to talk to Amazon EBS and manage the volume
    // on our behalf.
    try {
        const root = process.env.FENNEL_ROOT!;
        const policyFilePath = path.join(root, "/deployment/artifacts/volume-policy.yaml")
        var policyJson = fs.readFileSync(policyFilePath, 'utf8')
    } catch (err) {
        console.error(err)
        process.exit()
    }
    const iamPolicy = new aws.iam.Policy(`${getPrefix(input.scope, input.planeId)}-ebs-driver-policy`, {
        namePrefix: `${getPrefix(input.scope, input.planeId)}-EbsCsiDriverIAMPolicy`,
        policy: policyJson,
    }, { provider: awsProvider })

    const serviceAccountName = "ebs-csi-controller-sa"
    const namespace = "kube-system"

    const { role } = await setupIamRoleForServiceAccount(input, awsProvider,
        "csi-driver", namespace, serviceAccountName, cluster)

    const attachPolicy = new aws.iam.RolePolicyAttachment(`${getPrefix(input.scope, input.planeId)}-attach-ebs-policy`, {
        role: role.id,
        policyArn: iamPolicy.arn,
    }, { provider: awsProvider })

    // Install the driver.
    //
    // we should schedule all components of EBS CSI driver on ON_DEMAND instances
    const driver = new k8s.helm.v3.Release("ebs-csi-driver", {
        repositoryOpts: {
            repo: "https://kubernetes-sigs.github.io/aws-ebs-csi-driver/",
        },
        chart: "aws-ebs-csi-driver",
        namespace: namespace,
        version: "v2.8.1",
        values: {
            "controller": {
                "serviceAccount": {
                    "create": false,
                    "name": serviceAccountName,
                },
                "nodeSelector": {
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                }
            },
            "node": {
                "nodeSelector": {
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                }
            }
        }
    }, { provider: cluster.provider, dependsOn: attachPolicy })
}

// setupSpotRescheduler is responsible to move pods scheduled on on-demand nodes to spot nodes proactively
async function setupSpotRescheduler(awsProvider: aws.Provider, input: inputType, cluster: eks.Cluster,
    spotNodeLabel: string, onDemandNodeLabel: string) {
    // Account ID
    const current = await aws.getCallerIdentity({ provider: awsProvider });
    const accountId = current.accountId;

    // SpotRescheduler is doing a job which ideally cluster autoscaler should have done.
    // It requires more or less the same permissions as cluster autoscaler (mostly around interacting with
    // EC2 ASG, EKS, EKS Managed Node Groups and few AWS EC2 APIs to fetch supported instance types etc)
    //
    // See: https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html
    const roleName = `${getPrefix(input.scope, input.planeId)}-spot-rescheduler-role`;

    const role = pulumi.all([cluster.core.oidcProvider!.url, cluster.core.cluster.name]).apply(([oidcUrl, clusterName]) => {
        return new aws.iam.Role(roleName, {
            namePrefix: roleName,
            description: "IAM role for EKS cluster spot rescheduler",
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
                         "${oidcUrl}:sub": "system:serviceaccount:kube-system:spot-rescheduler"
                       }
                     }
                   }
                 ]
               }`,
            // Add `eks:DescribeNodeGroup` and `eks:ListNodeGroups` as additional permissions to allow list labels
            // attached only to managed node groups and not EC2 ASGs
            inlinePolicies: [{
                name: "eks-cluster-spot-rescheduler-policy",
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
                                "ec2:DescribeInstanceTypes",
                                "eks:DescribeNodegroup",
                                "eks:ListNodegroups"
                            ],
                            "Resource": "*"
                        }
                    ]
                }`,
            }],
        }, { provider: awsProvider });
    });

    // Setup the spot rescheduler
    return pulumi.all([role.arn, cluster.core.cluster.name]).apply(([roleArn, clusterName]) => {
        const autoscalerName = `${getPrefix(input.scope, input.planeId)}-cluster-spot-rescheduler`;
        return new k8s.helm.v3.Release(autoscalerName, {
            repositoryOpts: {
                "repo": "https://fennel-ai.github.io/public/helm-charts/spot-rescheduler/",
            },
            // this must match the namespace provided in the role above.
            namespace: "kube-system",
            chart: "spot-rescheduler",
            version: "0.1.5",
            values: {
                // auto-discover the autoscaling groups of the EKS cluster (since we use managed node groups, the necessary
                // tags (`k8s.io/cluster-autoscaler/enabled` and `k8s.io/cluster-autoscaler/<CLUSTER_NAME>`) are
                // already applied.
                "autoDiscovery": {
                    "clusterName": clusterName,
                },
                "image": {
                    "tag": "2.0.0",
                },
                "awsRegion": input.region,
                "cloudProvider": "aws",
                // TODO(mohit): Consider making this > 1
                "replicaCount": 1,
                "onDemandNodeLabel": onDemandNodeLabel,
                "spotNodeLabel": spotNodeLabel,
                "prometheusMetricPort": 8084,
                // autoscaler exports prometheus metrics, enable scraping them through our telemetry setup
                "podAnnotations": {
                    "prometheus.io/scrape": "true",
                    // the port is the default value for the port of the service
                    "prometheus.io/port": "8084",
                },
                // we should schedule all components of kubernetes cluster autoscaler on ON_DEMAND instances
                "nodeSelector": {
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                },
                // annotate the service account with the IAM role
                "rbac": {
                    "serviceAccount": {
                        // this must match the name provided above in the role.
                        "name": "spot-rescheduler",
                        "annotations": {
                            "eks.amazonaws.com/role-arn": roleArn,
                        }
                    }
                },
                // override the full name as the one created by the helm release is long and has redundant words
                "fullnameOverride": autoscalerName,
                "extraArgs": {
                    "housekeeping-interval": "2m",
                    "pod-eviction-timeout": "2m",
                    "node-scaleup-timeout": "5m",
                    "node-ready-check-interval": "10s",
                    "max-graceful-termination": "2m",
                    "node-drain-cooldown-interval": "1m",
                },
            }
        }, { provider: cluster.provider, deleteBeforeReplace: true });
    });
}

async function setupClusterAutoscaler(awsProvider: aws.Provider, input: inputType, cluster: eks.Cluster,
    nodeGroups: NodeGroupConf[]) {
    // Account ID
    const current = await aws.getCallerIdentity({ provider: awsProvider });
    const accountId = current.accountId;

    // See: https://docs.aws.amazon.com/eks/latest/userguide/autoscaling.html
    const roleName = `${getPrefix(input.scope, input.planeId)}-autoscaler-role`;

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
                                "autoscaling:DescribeScalingActivities",
                                "ec2:DescribeLaunchTemplateVersions",
                                "ec2:DescribeInstanceTypes",
                                "ec2:DescribeImages",
                                "ec2:GetInstanceTypesFromInstanceRequirements",
                                "eks:DescribeNodegroup"
                            ],
                            "Resource": "*"
                        }
                    ]
                }`,
            }],
        }, { provider: awsProvider });
    });

    // create a mapping from priority to list of node group name regex's for the expansion priority configmap in
    // cluster autoscaler
    //
    // Since EKS 1.21, autoscaling group names are of the format - `eks-<managed-node-group-name>-uuid`.
    // See - https://aws.amazon.com/blogs/containers/amazon-eks-1-21-released/
    // We will need to construct a regex of the form - `.*{managed-node-group-name}.*`. This creates a restriction that
    // node group name cannot have `.` because it needs to be formatted as `/.` for the regex to work
    // TODO(mohit): Consider adding support for this
    //
    // NOTE: this is required because priority expander has a requirement that "priority values cannot be duplicated"
    let expansionPriorities = new Map<string, string[]>();
    for (const nodeGroup of nodeGroups) {
        const priority = `${nodeGroup.expansionPriority}`;
        const nodeGroupNameRegex = `.*${nodeGroup.name}.*`;
        if (expansionPriorities.has(priority)) {
            // we just checked if the map has entries for priority, we can force a value and not worry about `undefined`
            let ngs = expansionPriorities.get(priority)!;
            ngs.push(nodeGroupNameRegex)
            expansionPriorities.set(priority, ngs);
        } else {
            expansionPriorities.set(priority, [nodeGroupNameRegex]);
        }
    }

    // TODO(mohit): Consider adding a placeholder regex `.*` with the least priority so that every node group in the
    // cluster is considered for expansion.
    //
    // Currently we configure all the node groups at the plane level and a regex is created for each. But in the "higher
    // availability" mode, we create a node group for envoy pods, which will miss out here for priority expansion.
    // This is expected for now as we do not want any other service/pod to run there, but we might in the future
    // consider autoscaling envoy pods as well.

    // convert to a record since helm charts seem to only work with records (maybe because of the difference in
    // map and record's string repr?)
    let expanderPriorities: Record<string, string[]> = {};
    for (let [priority, ngs] of expansionPriorities) {
        expanderPriorities[priority] = ngs
    }

    // Setup the cluster autoscaler
    //
    // Currently the cluster autoscaler ensures that none of the pods are un-schedulable.
    //
    // NOTE: our setup with node-selectors and affinity does not allow cluster autoscaler to run at it's full power.
    // This is currently setup along with Horizontal Pod Autoscaler which increases/decreases the pods, which could
    // require adding/removing a new node, which is actuated by the cluster autoscaler.
    return pulumi.all([role.arn, cluster.core.cluster.name]).apply(([roleArn, clusterName]) => {
        const autoscalerName = `${getPrefix(input.scope, input.planeId)}-cluster-autoscaler`;
        return new k8s.helm.v3.Release(autoscalerName, {
            repositoryOpts: {
                "repo": "https://kubernetes.github.io/autoscaler",
            },
            // this must match the namespace provided in the role above.
            namespace: "kube-system",
            chart: "cluster-autoscaler",
            // fix a version so that a plane update does not lead to an unintentional cluster autoscaler update
            version: "9.19.2",
            values: {
                // auto-discover the autoscaling groups of the EKS cluster (since we use managed node groups, the necessary
                // tags (`k8s.io/cluster-autoscaler/enabled` and `k8s.io/cluster-autoscaler/<CLUSTER_NAME>`) are
                // already applied.
                "autoDiscovery": {
                    "clusterName": clusterName,
                },
                // Use v1.24 image since it has the functionality to drop a node group for which scale up request failed
                //
                // This is possible when node groups comprise spot instances
                // See - https://github.com/kubernetes/autoscaler/pull/4489#issuecomment-1157754799
                "image": {
                    "tag": "v1.24.0",
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
                // we should schedule all components of kubernetes cluster autoscaler on ON_DEMAND instances
                "nodeSelector": {
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
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
                // Set expansion priorities for the cluster autoscaler to scale node groups based on the priorities
                // configured for them
                //
                // NOTE: Expander is usually the last factor considered in the scheduler i.e. say the workload
                // has other scheduler requirements (e.g. node selector, affinity/anti-affinity, gpu availability etc),
                // they will take precedence over the expander. Expander is used to select a node group with the
                // highest priority, matching all requirements
                //
                // NOTE: If a group name doesn't match any of the regular expressions in the priority list, it will
                // not be considered for expansion
                //
                // For more details see - https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/expander/priority/readme.md
                "expanderPriorities": expanderPriorities,
                // "extraArgs" needs to be set to tune the autoscaler as per:
                // https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-are-the-parameters-to-ca
                "extraArgs": {
                    // set priority based expander where the cluster autoscaler will expand the specified node groups
                    // based on the configured priority
                    "expander": "priority",
                    // How long a node should be unneeded before it is eligible for scale down. Default is 10m.
                    "scale-down-unneeded-time": "1m",
                    // How long after scale up that scale down evaluation resumes. Default is 10m.
                    "scale-down-delay-after-add": "3m",
                }
            }
        }, { provider: cluster.provider, deleteBeforeReplace: true });
    });
}

async function setupMetricsServer(provider: aws.Provider, input: inputType, cluster: eks.Cluster) {
    const metricServerName = `${getPrefix(input.scope, input.planeId)}-metrics-server`
    return new k8s.helm.v3.Release(metricServerName, {
        repositoryOpts: {
            repo: "https://kubernetes-sigs.github.io/metrics-server/"
        },
        chart: "metrics-server",
        namespace: "kube-system",
        version: "3.8.2",
        values: {
            "fullnameOverride": metricServerName,
            "args": [
                // this needs to be at least 10s - https://github.com/kubernetes-sigs/metrics-server/blob/master/cmd/metrics-server/app/options/options.go#L67
                "--metric-resolution=10s",
                "--kubelet-insecure-tls"
            ],
            // we should schedule all components of kubernetes metrics server on ON_DEMAND instances
            "nodeSelector": {
                "eks.amazonaws.com/capacityType": "ON_DEMAND",
            },
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
    const cluster = new eks.Cluster(`${getPrefix(input.scope, input.planeId)}-eks-cluster`, {
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
    const autoscaler = setupClusterAutoscaler(awsProvider, input, cluster, input.nodeGroups);

    // setup spot rescheduler
    if (input.spotReschedulerConf !== undefined) {
        const rescheduler = setupSpotRescheduler(awsProvider, input, cluster, input.spotReschedulerConf.spotNodeLabel,
            input.spotReschedulerConf.onDemandNodeLabel);
    }

    // setup metrics server for autoscaling needs
    const metricsServer = setupMetricsServer(awsProvider, input, cluster);

    // Get the cluster security group created by EKS for managed node groups and fargate.
    // Source: https://docs.aws.amazon.com/eks/latest/userguide/sec-group-reqs.html
    const clusterSg = cluster.eksCluster.vpcConfig.clusterSecurityGroupId;

    const instanceRole = cluster.core.instanceRoles.apply((roles) => { return roles[0].name })
    const instanceRoleArn = cluster.core.instanceRoles.apply((roles) => { return roles[0].arn })

    try {
        var publicKey: string = fs.readFileSync("../eks/ssh_keypair/id_rsa.pub", "utf-8")
    } catch (err) {
        console.log("Failed to read key-pair: " + err)
        exit(1)
    }
    const keyPair = new aws.ec2.KeyPair(`${getPrefix(input.scope, input.planeId)}-eks-workers`, { publicKey: publicKey }, { provider: awsProvider })
    // Setup managed node groups
    for (let nodeGroup of input.nodeGroups) {
        if (nodeGroup.capacityType === SPOT_INSTANCE_TYPE && nodeGroup.instanceTypes.length <= 1) {
            console.warn(`consider specifying > 1 instance type for node group with SPOT capacity type. node group: ${nodeGroup.name}`)
        }
        if (nodeGroup.capacityType === ON_DEMAND_INSTANCE_TYPE && nodeGroup.instanceTypes.length != 1) {
            console.error(`node group with capacity type ON_DEMAND should have a single instance type. node group: ${nodeGroup.name}`)
            process.exit(1)
        }
        const n = new eks.ManagedNodeGroup(nodeGroup.name, {
            // Todo(Amit): Stop using fixed RSA keys and migrate to Tailscale.
            // Following were the hard part of getting things up and running with tailscale.
            // 1. Need to figure out if every node should be running tailscale SSH or
            //    one node per plane serving as reverse tunnel to the other nodes.
            // 2. Bootstrapping tailscale needs running custom script on launch. ManagedNodeGroup does provide
            //    that mechanism using NodeLaunchTemplate and ec2.LaunchTemplate, but setting all this
            //    up becomes too configuration heavy and prone to errors and needs careful evaluation.
            remoteAccess: {
                ec2SshKey: keyPair.keyName
            },
            cluster: cluster,
            scalingConfig: {
                // desired size should be set only for scenarios where the cluster should start with a certain set
                // of nodes but this should not be changed when cluster autoscaler is setup
                //
                // this field is optional and should ideally not be set when cluster autoscaler is configured;
                // see: https://docs.aws.amazon.com/eks/latest/APIReference/API_NodegroupScalingConfig.html
                //
                // NOTE: This field is optional from AWS but pulumi marks this field as required, we set this to minSize
                // and expect that the cluster autoscaler will scale this up
                desiredSize: nodeGroup.minSize,
                minSize: nodeGroup.minSize,
                maxSize: nodeGroup.maxSize,
            },
            // accepts multiple strings but the EKS API accepts only a single string
            instanceTypes: nodeGroup.instanceTypes,
            nodeGroupNamePrefix: nodeGroup.name,
            labels: nodeGroup.labels,
            nodeRoleArn: instanceRoleArn,
            subnetIds: privateSubnets,
            amiType: nodeGroup.amiType,
            // this specifies if the instances in this node group should be SPOT or ON_DEMAND
            capacityType: nodeGroup.capacityType,
        }, { provider: awsProvider });
    }

    // Install descheduler.
    setupDescheduler(cluster);

    // Connect cluster node security group to connected vpcs.
    if (input.connectedVpcCidrs !== undefined) {
        const sgRules = new aws.ec2.SecurityGroupRule(`${getPrefix(input.scope, input.planeId)}-eks-sg-rule`, {
            type: "ingress",
            fromPort: 0,
            toPort: 65535,
            protocol: "tcp",
            cidrBlocks: input.connectedVpcCidrs,
            securityGroupId: clusterSg,
        }, { provider: awsProvider })
    }

    const policy = new aws.iam.RolePolicy(`${getPrefix(input.scope, input.planeId)}-s3-createbucket-rolepolicy`, {
        name: `${getPrefix(input.scope, input.planeId)}-s3-createbucket-rolepolicy`,
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
    // await setupEmissaryIngressCrds(input, awsProvider, cluster)

    // Setup fennel namespace.
    const ns = new k8s.core.v1.Namespace("fennel-ns", {
        metadata: {
            name: "fennel",
            annotations: {
                "linkerd.io/inject": "enabled",
            },
        }
    }, { provider: cluster.provider })

    // Setup AWS EBS CSI driver.
    setupEbsCsiDriver(input, awsProvider, cluster)

    // Setup storageclasses to be used by stateful sets.
    const storageclasses = setupStorageClasses(cluster)
    const clusterName = cluster.core.cluster.name
    // Setup provisioner for deploying PVs backed by locally attached disks.
    await setupEKSLocalSSDProvisioner(cluster, awsProvider);
    storageclasses["local"] = pulumi.output("nvme-ssd")

    const output = pulumi.output({
        kubeconfig, oidcUrl, instanceRole, instanceRoleArn, clusterSg, clusterName, storageclasses,
    })

    return output
}
