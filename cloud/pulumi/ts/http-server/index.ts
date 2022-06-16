import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";
import * as childProcess from "child_process";
import {serviceEnvs} from "../tier-consts/consts";

const name = "http-server"

export const plugins = {
    "kubernetes": "v3.18.0",
    "docker": "v3.1.0",
    "aws": "v5.1.0"
}

const DEFAULT_REPLICAS = 2
const DEFAULT_FORCE_REPLICA_ISOLATION = false

export type inputType = {
    region: string,
    roleArn: string,
    kubeconfig: string,
    namespace: string,
    tierId: number,
    replicas?: number,
    enforceReplicaIsolation?: boolean,
    nodeLabels?: Record<string, string>,
}

export type outputType = {
    appLabels: {[key: string]: string},
    svc: pulumi.Output<k8s.core.v1.Service>,
}

export const setup = async (input: inputType) => {
    const awsProvider = new aws.Provider("http-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`t-${input.tierId}-http-server-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    const repoPolicy = new aws.ecr.LifecyclePolicy(`t-${input.tierId}-http-server-repo-policy`, {
        repository: repo.name,
        policy: {
            rules: [{
                // sets the order in which rules are applied; this rule will be applied first
                rulePriority: 1,
                description: "Policy to expire images after 120 days",
                selection: {
                    // since we don't deterministically know the tag prefix, we use "any" -> both tagged and untagged
                    // images are considered
                    tagStatus: "any",
                    // limits since when the image was pushed
                    countType: "sinceImagePushed",
                    // set 120 days as the ttl
                    countUnit: "days",
                    countNumber: 120,
                },
                action: {
                    type: "expire"
                },
            }],
        }
    }, { provider: awsProvider });

    // Get registry info (creds and endpoint).
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

    const root = process.env["FENNEL_ROOT"]!
    // Get the (hash) commit id.
    // NOTE: This requires git to be installed and DOES NOT take local changes or commits into consideration.
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageName = repo.repositoryUrl.apply( imgName => {
        return `${imgName}:${hashId}`
    })

    // Build and publish the container image.
    const image = new docker.Image("http-server-img", {
        build: {
            context: root,
            dockerfile: path.join(root, "dockerfiles/http.dockerfile"),
            args: {
                "platform": "linux/amd64",
            },
        },
        imageName: imageName,
        registry: registryInfo,
    });

    const k8sProvider = new k8s.Provider("httpserver-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const metricsPort = 2112;

    // if node labels are specified, create an affinity for the pod towards that node (or set of nodes)
    let affinity: k8s.types.input.core.v1.Affinity = {};
    if (input.nodeLabels !== undefined) {
        let terms: k8s.types.input.core.v1.NodeSelectorTerm[] = [];

        // Terms are ORed i.e. if there are 2 node labels mentioned, if there is a node with either (or both) the
        // nodes, the pod is scheduled on it.
        Object.entries(input.nodeLabels).forEach(([labelKey, labelValue]) => terms.push({
            matchExpressions: [{
                key: labelKey,
                operator: "In",
                values: [labelValue],
            }],
        }));

        affinity.nodeAffinity = {
            requiredDuringSchedulingIgnoredDuringExecution: {
                nodeSelectorTerms: terms,
            },
        }
    }

    const forceReplicaIsolation = input.enforceReplicaIsolation || DEFAULT_FORCE_REPLICA_ISOLATION;
    let whenUnsatisfiable = "ScheduleAnyway";
    if (forceReplicaIsolation) {
        whenUnsatisfiable = "DoNotSchedule";
    }

    const httpServerDepName = "http-server";
    const appDep = image.imageName.apply(() => {
        return new k8s.apps.v1.Deployment("http-server-deployment", {
            metadata: {
                name: httpServerDepName,
            },
            spec: {
                selector: { matchLabels: appLabels },
                // NOTE: If changing number replicas, please take: size and desired capacity of the nodegroup,
                //
                // NOTE: If changing number replicas, please take `topologySpreadConstraints`
                // into consideration which schedules replicas on different nodes.
                replicas: input.replicas || DEFAULT_REPLICAS,
                // TODO: eventually remove this.
                //
                // configure one of the existing pods to go down before k8s scheduler tries to schedule a new
                // pod - this is required since with the current setup of:
                //  1. X nodes
                //  2. <= X replicas, with the requirement that each replica is scheduled on different nodes
                //
                // might run into a scheduling problem if new pods are brought up before old pods are descheduled
                strategy: {
                    rollingUpdate: {
                        maxSurge: 0,
                        maxUnavailable: 1,
                    }
                },
                template: {
                    metadata: {
                        labels: appLabels,
                        annotations: {
                            // Skip Linkerd protocol detection for mysql and redis
                            // instances running outside the cluster.
                            // See: https://linkerd.io/2.11/features/protocol-detection/.
                            "config.linkerd.io/skip-outbound-ports": "3306,6379",
                            "prometheus.io/scrape": "true",
                            "prometheus.io/port": metricsPort.toString(),
                        }
                    },
                    spec: {
                        affinity: affinity,
                        // https://kubernetes.io/docs/concepts/workloads/pods/pod-topology-spread-constraints/
                        topologySpreadConstraints: [
                            // describes how a group of pods ought to spread across topology domains.
                            // Scheduler will schedule pods in a way which abides by the constraints.
                            // All the constraints are ANDed
                            {
                                // describes the degree to which pods may be unevenly distributed.
                                // it is the maximum permitted difference between the number of matching pods in the
                                // target topology and the global minimum.
                                maxSkew: 1,
                                // key of the node labels. we check by the host name.
                                topologyKey: "kubernetes.io/hostname",
                                // schedule anyway on the pod when constraints are not satisfied - to avoid potential
                                // contention b/w pods. This is to avoid scheduling multiple http-server pods
                                // from different namespaces on the same data plane.
                                whenUnsatisfiable: whenUnsatisfiable,
                                // find matching pods using the labels - `appLabels`
                                //
                                // this should schedule the replicas across different nodes
                                labelSelector: {
                                    matchLabels: appLabels,
                                },
                            }
                        ],
                        containers: [{
                            command: [
                                "/root/server",
                                "--metrics-port",
                                "2112",
                                "--dev=false"
                            ],
                            name: name,
                            image: image.imageName,
                            imagePullPolicy: "Always",
                            ports: [
                                {
                                    containerPort: 2425,
                                    protocol: "TCP",
                                },
                                {
                                    containerPort: metricsPort,
                                    protocol: "TCP",
                                },
                            ],
                            env: serviceEnvs,
                        },],
                    },
                },
            },
        }, { provider: k8sProvider, deleteBeforeReplace: true });
    })

    const appSvc = appDep.apply(() => {
        return new k8s.core.v1.Service("http-svc", {
            metadata: {
                labels: appLabels,
                name: name,
            },
            spec: {
                type: "ClusterIP",
                ports: [{ port: 2425, targetPort: 2425, protocol: "TCP" }],
                selector: appLabels,
            },
        }, { provider: k8sProvider, deleteBeforeReplace: true })
    })

    // Setup ingress resources for http-server.
    const mapping = new k8s.apiextensions.CustomResource("api-server-mapping", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Mapping",
        metadata: {
            name: "data-server-mapping",
            labels: {
                "svc": "go-http",
            }
        },
        spec: {
            "hostname": "*",
            "prefix": "/data/",
            "service": "http-server:2425",
            "timeout_ms": 30000,
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const host = new k8s.apiextensions.CustomResource("api-server-host", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Host",
        metadata: {
            name: "api-server-host",
            labels: {
                "svc": "go-http",
            }
        },
        spec: {
            "hostname": "*",
            "acmeProvider": {
                "authority": "none",
            },
            "tlsSecret": {
                "name": "tls-cert",
            },
            "tls": {
                "min_tls_version": "v1.2",
                "alpn_protocols": "h2",
            },
            "mappingSelector": {
                "matchLabels": {
                    "svc": "go-http",
                }
            },
            "requestPolicy": {
                "insecure": {
                    "action": "Route",
                }
            }
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    // configure horizontal pod autoscaler for the query-server deployment/pod
    //
    // we can not use autoscaling/v2 because since we are limited by EKS support for only v1.22.9. v2 is supported from
    // v1.23
    const podAutoscaler = new k8s.autoscaling.v2beta2.HorizontalPodAutoscaler("http-server-hpa", {
        metadata: {
            name: "http-server-hpa",
        },
        spec: {
            scaleTargetRef: {
                kind: "Deployment",
                name: httpServerDepName,
            },
            // we will keep the default behavior for scaling up and down. We should tune this per-service
            // based on the behavior we notice
            //
            // default behavior for scaleDown is to allow to scale down to `minReplicas` pods, with a
            // 300 second stabilization window (i.e., the highest recommendation for the last 300sec is used)
            //
            // default behavior for scaleUp is the higher of:
            //  i) increase no more than 4 pods per 60 seconds
            //  ii) double the number of pods per 60 seconds; No stabilization is used
            //
            // stabilization is the number of seconds for which past recommendation should be considered while
            // scaling up or down. If it is set to ZERO, no stabilization is done i.e. the latest recommendation
            // is considered
            //
            // behavior: {}

            // spec used to calculate the replica count (maximum replica count across all the metrics will be used).
            //
            // replica count is calculated as (current value / target value) * #pods
            //
            // metrics used must decrease by increasing the pod count or vice-versa
            //
            // TODO: Explore other dimensions (e.g. external metrics, ingress metrics etc)
            // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/#autoscaling-on-multiple-metrics-and-custom-metrics
            metrics: [
                {
                    type: "Resource",
                    resource: {
                        name: "cpu",
                        target: {
                            type: "Utilization",
                            // value is in %
                            averageUtilization: 80
                        }
                    }
                },
                {
                    type: "Resource",
                    resource: {
                        name: "memory",
                        target: {
                            type: "Utilization",
                            // value is in %
                            // leave a buffer of 20% here so that, in the worst case, there is enough time
                            // for a node to spin up and pod getting scheduled on it
                            averageUtilization: 80
                        }
                    }
                }
            ],
            // currently we set this to 1 so that at-least one replica is always available, but explore if
            // (and which) services could be scaled down to ZERO.
            minReplicas: 1,
            maxReplicas: 5,
        },
    },{ provider: k8sProvider });

    const output: outputType = {
        appLabels: appLabels,
        svc: appSvc,
    }
    return output
}
