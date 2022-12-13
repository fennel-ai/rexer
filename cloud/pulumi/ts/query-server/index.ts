import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";
import {ReadinessProbe, serviceEnvs} from "../tier-consts/consts";
import * as util from "../lib/util";
import * as uuid from "uuid";
import childProcess from "child_process";

const name = "query-server"

export const plugins = {
    "kubernetes": "v3.20.1",
    "aws": "v5.1.0"
}

const DEFAULT_MIN_REPLICAS = 1
const DEFAULT_MAX_REPLICAS = 2

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
    enableCors?: boolean,
    minReplicas?: number,
    maxReplicas?: number,
    useAmd64?: boolean,
    nodeLabels?: Record<string, string>,
    resourceConf?: util.ResourceConf,
    pprofHeapAllocThresholdMegaBytes?: number,
}

export type outputType = {
    appLabels: { [key: string]: string },
    svc: k8s.core.v1.Service,
}

export const setup = async (input: inputType) => {

    // TODO(mohit): Consider making this a library! There is too much duplication in http-server and query-server
    // which could lead to divergence b/w them and some unintended issues

    const awsProvider = new aws.Provider("query-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`t-${input.tierId}-query-server-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    const repoPolicy = new aws.ecr.LifecyclePolicy(`t-${input.tierId}-query-server-repo-policy`, {
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

    let nodeSelector = input.nodeLabels || {};

    // NOTE: We do not set `CapacityType` for node selector configuration for Query servers as we want to run a
    // hybrid setup for it i.e. have a few replicas running on ON_DEMAND instances to have availability all the time
    // but run most of the workload on spot instances for cost efficiency

    // Build and publish the container image.
    const root = process.env["FENNEL_ROOT"]!;
    const dockerfile = path.join(root, 'dockerfiles/http_multiarch.dockerfile');
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageNameWithTag = repo.repositoryUrl.apply(iName => {
        return `${iName}:${hashId}-${uuid.v4()}`;
    });

    const imgBuildPush = util.BuildMultiArchImage("query-server-img", root, dockerfile, imageNameWithTag);

    const k8sProvider = new k8s.Provider("queryserver-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const metricsPort = 2112;
    const appPort = 2425;
    const healthPort = 8082;

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

    const queryServerDepName = "query-server";
    let envVars: pulumi.Input<k8s.types.input.core.v1.EnvVar>[] = serviceEnvs;
    if (input.pprofHeapAllocThresholdMegaBytes !== undefined) {
        envVars.push({
            name: "PPROF_HEAP_ALLOC_THRESHOLD_MEGABYTES",
            value: `${input.pprofHeapAllocThresholdMegaBytes}`
        })
    }

    const appDep = new k8s.apps.v1.Deployment("query-server-deployment", {
        metadata: {
            name: queryServerDepName,
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
                        "config.linkerd.io/proxy-cpu-request": "0.75",
                        "config.linkerd.io/proxy-memory-limit": "2Gi",
                        "config.linkerd.io/proxy-memory-request": "128Mi",
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
                            "--health-port",
                            `${healthPort}`,
                            "--dev=false"
                        ],
                        name: name,
                        image: imageNameWithTag,
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
                            {
                                containerPort: healthPort,
                                protocol: "TCP",
                            }
                        ],
                        env: envVars,
                        resources: {
                            requests: {
                                "cpu": input.resourceConf?.cpu.request || DEFAULT_CPU_REQUEST,
                                "memory": input.resourceConf?.memory.request || DEFAULT_MEMORY_REQUEST,
                            },
                            limits: {
                                "cpu": input.resourceConf?.cpu.limit || DEFAULT_CPU_LIMIT,
                                "memory": input.resourceConf?.memory.limit || DEFAULT_MEMORY_LIMIT,
                            }
                        },
                        readinessProbe: ReadinessProbe(healthPort),
                    },],
                    // this should be at least the timeout seconds so that any new request sent to the container
                    // could take this much time + `preStop` on linkerd is an artificial delay added to avoid
                    // failing requests downstream.
                    terminationGracePeriodSeconds: timeoutSeconds + linkerdPreStopDelaySecs,
                },
            },
            strategy: {
                type: "RollingUpdate",
                rollingUpdate: {
                    maxSurge: 1,
                    maxUnavailable: 1,
                },
            }
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true, dependsOn: imgBuildPush });

    const appSvc = new k8s.core.v1.Service("query-svc", {
        metadata: {
            labels: appLabels,
            name: name,
        },
        spec: {
            type: "ClusterIP",
            ports: [{ port: appPort, targetPort: appPort, protocol: "TCP" }],
            selector: appLabels,
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true, dependsOn: appDep });

    // Create kubernetes endpoint resolver, which configures emissary to resolve kubernetes endpoints.
    const endpointResolverName = 'query-server-endpoint-resolver';
    const resolver = new k8s.apiextensions.CustomResource("query-server-resolver", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "KubernetesEndpointResolver",
        metadata: {
            name: endpointResolverName,
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    // Setup ingress resources for query-server.
    let spec: Record<string, any> = {
        "hostname": "*",
        "prefix": "\/data\/(internal\/v1\/query|v1\/query|query)",
        "prefix_regex": true,
        "rewrite": "/query",
        "service": `query-server:${appPort}`,
        "timeout_ms": timeoutSeconds * 1000,
        "retry_policy": {
            // Retry on gateway errors (which applies to 502, 503 or 504 responses)
            //
            // See - https://www.getambassador.io/docs/emissary/latest/topics/using/retries/#retry_on
            //
            // Also see - https://www.envoyproxy.io/docs/envoy/latest/faq/load_balancing/transient_failures#retries
            //
            // Ideally we want to retry on `connect-failure` and `retriable-4xx` errors as well, but
            // emissary does not support configuring multiple `retry_on`
            //
            // (request retry section) - that multiple `retry_on` are in fact possible to configure
            "retry_on": "gateway-error",
            // Retry twice at max
            //
            // Currently we have not ruled out the exact cause for query servers OOMing. If the root cause is that
            // there is a query of death (=> a query which requires a large memory allocation in this case),
            // retrying many times will lead to multiple servers crashing with OOMs
            //
            // NOTE: we currently do not set `max_retry` in the global circuit breaker configured and it defaults to
            // 5. This should be lower than the value set there so that the circuit breaker does not preempt
            // the request before desired (or Mapping configured) retries
            "num_retries": 2,
            // Use `per_try_timeout` - specifies the timeout for each retry.
            // Default: this is the global request timeout (which is by default 3000ms, and is overridden per
            // mapping)
            "per_try_timeout": "60s"
        },
        "circuit_breakers": [{
            // Specifies the maximum number of concurrent retries there could be to the upstream service
            //
            // We noticed that this limit was being hit for many 5xx failures and there were no retry attempts,
            // increasing it reasonably so that - the budget is not hit frequently, but also considering
            // too many retries could potentially kill other servers (as the root cause for these failures in the
            // first place is because of servers restarting)
            //
            // defaults to 3
            "max_retries": 25,
        }],
        // use kubernetes endpoint level discovery so that the load balancing decisions are taken by
        // emissary (or envoy) and we can configure advanced load balancing algorithms
        "resolver": endpointResolverName,
        "load_balancer": {
            // Since we have not configured any weights for the endpoints, this should effectively
            // behave like P2C (power of 2 choices) in which 2 endpoints are picked at random and one with the least
            // requests is used to forward the request. This should help us more or less equally distribute
            // the requests but also ensure the queue length (and potentially the latency) into consideration
            // while load balancing.
            //
            // See - https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/load_balancing/load_balancers#weighted-least-request
            //
            // Also see - https://blog.envoyproxy.io/examining-load-balancing-algorithms-with-envoy-1be643ea121c
            // which gives a good idea about how different algorithms behave.
            //
            // NOTE: This is may not an ideal load balancing algorithms when say a "node" or "endpoint" is
            // constantly failing and fails immediately hence it's queue length is almost always lower than any
            // other node, if this is a contender in the 2 endpoints selected, this will always be selected,
            // hence failing the requests. A good solution for this is "outlier detection" mechanism in
            // envoy but emissary does not allow us to configure them
            "policy": "least_request"
        }
    };
    if (input.enableCors) {
        spec["cors"] = {
            // allow requests from any origin
            "origins": ["*"],
            // allow only GET POST and OPTIONS methods
            "methods": ["GET", "POST", "OPTIONS"],
        }
    }
    const mapping = new k8s.apiextensions.CustomResource("query-server-mapping", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Mapping",
        metadata: {
            name: "query-server-mapping",
            labels: {
                "svc": "go-query",
            }
        },
        spec: spec,
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const host = new k8s.apiextensions.CustomResource("query-server-host", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Host",
        metadata: {
            name: "query-server-host",
            labels: {
                "svc": "go-query",
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
                    "svc": "go-query",
                }
            },
            "requestPolicy": {
                "insecure": {
                    "action": "Route",
                }
            }
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    // TODO(mohit): Consider making this a library for other pods/deployments to use passing a simple configuration

    // configure horizontal pod autoscaler for the query-server deployment/pod
    //
    // we can not use autoscaling/v2 because since we are limited by EKS support for only v1.22.9. v2 is supported from
    // v1.23
    const podAutoscaler = new k8s.autoscaling.v2beta2.HorizontalPodAutoscaler("query-server-hpa", {
        metadata: {
            name: "query-server-hpa",
        },
        spec: {
            scaleTargetRef: {
                apiVersion: "apps/v1",
                kind: "Deployment",
                name: queryServerDepName,
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
                            //
                            // See - https://linear.app/fennel-ai/issue/REX-1414#comment-ad1dcb5b
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
