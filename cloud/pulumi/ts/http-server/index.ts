import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";
import * as childProcess from "child_process";
import { serviceEnvs } from "../tier-consts/consts";
import * as util from "../lib/util";

const name = "http-server"

export const plugins = {
    "kubernetes": "v3.18.0",
    "docker": "v3.1.0",
    "aws": "v5.1.0"
}

const DEFAULT_MIN_REPLICAS = 1
const DEFAULT_MAX_REPLICAS = 2
const DEFAULT_USE_AMD64 = false

// default for resource requirement configurations
const DEFAULT_CPU_REQUEST = "200m"
const DEFAULT_CPU_LIMIT = "1000m"
const DEFAULT_MEMORY_REQUEST = "500M"
const DEFAULT_MEMORY_LIMIT = "2G"

export type inputType = {
    region: string,
    roleArn: string,
    kubeconfig: string,
    namespace: string,
    tierId: number,
    minReplicas?: number,
    maxReplicas?: number,
    useAmd64?: boolean,
    nodeLabels?: Record<string, string>,
    resourceConf?: util.ResourceConf
}

export type outputType = {
    appLabels: { [key: string]: string },
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

    let nodeSelector = input.nodeLabels || {};
    const root = process.env["FENNEL_ROOT"]!
    // Get the (hash) commit id.
    // NOTE: This requires git to be installed and DOES NOT take local changes or commits into consideration.
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageName = repo.repositoryUrl.apply(imgName => {
        return `${imgName}:${hashId}`
    })

    let dockerfile, platform;
    if (input.useAmd64 || DEFAULT_USE_AMD64) {
        dockerfile = path.join(root, "dockerfiles/http.dockerfile")
        platform = "linux/amd64"
        nodeSelector["kubernetes.io/arch"] = "amd64"
    } else {
        dockerfile = path.join(root, "dockerfiles/http_arm64.dockerfile")
        platform = "linux/arm64"
        nodeSelector["kubernetes.io/arch"] = "arm64"
    }

    // Build and publish the container image.
    const image = new docker.Image("http-server-img", {
        build: {
            context: root,
            dockerfile: dockerfile,
            args: {
                "platform": platform,
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
    const appPort = 2425;

    const timeoutSeconds = 60;
    // NOTE: This is configured for "slow" clients who might, at the time of graceful shutdown (i.e. when the kubelet
    // has asked the container runtime to trigger TERM), since see this pod as a viable endpoint of the service.
    //
    // Linkerd as part of graceful shutdown does not accept any new requests (failing the request in this case), nor
    // does it allow the container running in the same pod to establish new network connections.
    //
    // A deeper dive:
    //  1. Envoy (shipped as part of Emissary Ingress and meshed with linkerd itself) uses linkerd proxy's service
    //      discovery data to route traffic.
    //  2. Linkerd service discovery involves the proxies registered with the linkerd destination, which receives
    //      updates from "EndpointsWatcher" which maintains a cache of the endpoints of the service, updating it
    //      from the data it fetches from API server. This information is streamed from linkerd destination to
    //      linkerd proxies. See: https://linkerd.io/2020/11/23/topology-aware-service-routing-on-kubernetes-with-linkerd/#putting-it-all-together-service-topology-in-linkerd
    //  3. Kubernetes, when a pod is scheduled for deletion or eviction, adds an entry in the API server (probably in etcd hosts).
    //      Kubelet running on the node, on which the pod is scheduled, notices this entry to invoke the container runtime
    //      to trigger "TERM" on the containers of the pod. See: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination
    //
    // Given a race condition in 2. and 3. it is possible that a proxy (running on envoy) thinks that an
    // endpoint corresponding to the pod shutting down is active, and send the request, which is rejected by the linkerd
    // proxy running on that pod.
    //
    // We set a delay of 1 sec, so that any calls made by the main container do not see a huge latency. We can also
    // remove this as a race b/w 2. and 3. is less likely, additionally this scenario is only noticed during pod termination.
    const linkerdPreStopDelaySecs = 1;

    const httpServerDepName = "http-server";
    const appDep = image.imageName.apply(() => {
        return new k8s.apps.v1.Deployment("http-server-deployment", {
            metadata: {
                name: httpServerDepName,
            },
            spec: {
                selector: { matchLabels: appLabels },
                // We skip setting replicas since the horizontal pod autoscaler is responsible on scheduling the
                // pods based on the utilization as configured in the HPA
                //
                // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale/#migrating-deployments-and-statefulsets-to-horizontal-autoscaling
                //
                // replicas:
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
                            // set linkerd proxy CPU and Memory requests and limits
                            //
                            // these needs to be set for Horizontal Pod Autoscaler to monitor and scale the pods
                            // (we configure the Horizontal Pod Autoscaler on the Deployment, which has 2 containers
                            // and for the metric server to scrape and monitor the resource utilization, it requires
                            // the limits for both to be reported).
                            //
                            // If we see any performance degradation due to the limits set here, we should increase them
                            // See - https://linkerd.io/2.9/tasks/configuring-proxy-concurrency/#using-kubernetes-cpu-limits-and-requests
                            "config.linkerd.io/proxy-cpu-limit": "1",
                            "config.linkerd.io/proxy-cpu-request": "0.25",
                            "config.linkerd.io/proxy-memory-limit": "1G",
                            "config.linkerd.io/proxy-memory-request": "128M",
                            // See: https://linkerd.io/2.11/tasks/graceful-shutdown/
                            "config.alpha.linkerd.io/proxy-wait-before-exit-seconds": linkerdPreStopDelaySecs.toString(),
                        }
                    },
                    spec: {
                        nodeSelector: nodeSelector,
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
                                    containerPort: appPort,
                                    protocol: "TCP",
                                },
                                {
                                    containerPort: metricsPort,
                                    protocol: "TCP",
                                },
                            ],
                            env: serviceEnvs,
                            resources: {
                                requests: {
                                    "cpu": input.resourceConf?.cpu.request || DEFAULT_CPU_REQUEST,
                                    "memory": input.resourceConf?.memory.request || DEFAULT_MEMORY_REQUEST,
                                },
                                limits: {
                                    "cpu": input.resourceConf?.cpu.limit || DEFAULT_CPU_LIMIT,
                                    "memory": input.resourceConf?.memory.limit || DEFAULT_MEMORY_LIMIT,
                                }
                            }
                        },],
                        // this should be at least the timeout seconds so that any new request sent to the container
                        // could take this much time + `preStop` on linkerd is an artificial delay added to avoid
                        // failing requests downstream.
                        terminationGracePeriodSeconds: timeoutSeconds + linkerdPreStopDelaySecs,
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
                ports: [{ port: appPort, targetPort: appPort, protocol: "TCP" }],
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
            "service": `http-server:${appPort}`,
            "timeout_ms": timeoutSeconds * 1000,
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

    // configure horizontal pod autoscaler for the http-server deployment/pod
    //
    // we can not use autoscaling/v2 because since we are limited by EKS support for only v1.22.9. v2 is supported from
    // v1.23
    const podAutoscaler = new k8s.autoscaling.v2beta2.HorizontalPodAutoscaler("http-server-hpa", {
        metadata: {
            name: "http-server-hpa",
        },
        spec: {
            scaleTargetRef: {
                apiVersion: "apps/v1",
                kind: "Deployment",
                name: httpServerDepName,
            },

            behavior: {
                scaleDown: {
                    policies: [
                        {
                            // Scale down by 1 pod in 120 seconds, this will essentially restrict scaling
                            // down the pods by 1 every 2 minutes even if the target replica count differs by > 1.
                            //
                            // `periodSeconds` indicates the length of time in the past for which the policy
                            // must hold true.
                            periodSeconds: 120,
                            type: "Pods",
                            value: 1,
                        }
                    ],
                    // This is redundant since we set a single policy
                    selectPolicy: "Min",
                    // This helps restrict "flapping" replica count due to fluctuating metric values during live
                    // traffic. When the metrics indicate that the target should be scaled down the algorithm
                    // looks into previously computed desired states, use the HIGHEST value in the past
                    // `stabilizationWindowSeconds` seconds.
                    //
                    // We set this to 600, so that we don't scale down aggressively during their customers live
                    // traffic - however, this will still scale down to the desired replica count eventually
                    // if we don't see any traffic (hence utilization).
                    stabilizationWindowSeconds: 600,
                }
                // default behavior for scaleUp is the higher of:
                //  i) increase no more than 4 pods per 60 seconds
                //  ii) double the number of pods per 60 seconds; No stabilization is used
                //
                // stabilization is set to ZERO, no stabilization is done i.e. the latest recommendation
                // is considered
            },

            // spec used to calculate the replica count (maximum replica count across all the metrics will be used).
            //
            // replica count is calculated as (current value / target value) * #pods
            //
            // metrics used must decrease by increasing the pod count or vice-versa
            //
            // TODO: Explore other dimensions (e.g. external metrics, ingress metrics etc)
            // https://kubernetes.io/docs/tasks/run-application/horizontal-pod-autoscale-walkthrough/#autoscaling-on-multiple-metrics-and-custom-metrics
            metrics: [
                // TODO(mohit): Explore configuring horizontal pod autoscaler once EKS supports kubernetes feature flags
                // See: https://github.com/aws/containers-roadmap/issues/512
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
            minReplicas: input.minReplicas || DEFAULT_MIN_REPLICAS,
            maxReplicas: input.maxReplicas || DEFAULT_MAX_REPLICAS,
        },
    }, { provider: k8sProvider });

    const output: outputType = {
        appLabels: appLabels,
        svc: appSvc,
    }
    return output
}
