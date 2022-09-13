import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws";
import * as k8s from "@pulumi/kubernetes";
import * as path from "path";
import * as process from "process";
import * as childProcess from "child_process";
import * as util from "../lib/util";

const name = "query-server-shadow"

export const plugins = {
    "kubernetes": "v3.20.1",
    "docker": "v3.1.0",
    "aws": "v5.1.0"
}

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
    shadowBucketName: string,
    nodeLabels?: Record<string, string>,
    resourceConf?: util.ResourceConf,
}

export type outputType = {}

export const setup = async (input: inputType) => {
    const awsProvider = new aws.Provider("query-server-shadow-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`t-${input.tierId}-query-server-shadow-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    const repoPolicy = new aws.ecr.LifecyclePolicy(`t-${input.tierId}-query-server-shadow-repo-policy`, {
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

    nodeSelector["kubernetes.io/arch"] = "arm64"
    // TODO(mohit): Consider making this a pod level configuration so that we can cut costs for tiers which
    // need not be available all the time e.g. demo tiers
    //
    // we should schedule all components of HTTP server on ON_DEMAND instances
    nodeSelector["eks.amazonaws.com/capacityType"] = "ON_DEMAND";

    // Build and publish the container image.
    const image = new docker.Image("query-server-shadow-img", {
        build: {
            context: root,
            dockerfile: path.join(root, "dockerfiles/traffic_capture.dockerfile"),
            args: {
                "platform": "linux/arm64",
            },
        },
        imageName: imageName,
        registry: registryInfo,
    });

    const k8sProvider = new k8s.Provider("query-server-shadow-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const appPort = 2429;

    const timeoutSeconds = 60;
    const linkerdPreStopDelaySecs = 1;

    const queryServerShadowDepName = "query-server-shadow";
    const appDep = image.imageName.apply(() => {
        return new k8s.apps.v1.Deployment("query-server-shadow-deployment", {
            metadata: {
                name: queryServerShadowDepName,
            },
            spec: {
                replicas: 1,
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
                            "linkerd.io/inject": "disabled",
                        }
                    },
                    spec: {
                        nodeSelector: nodeSelector,
                        containers: [{
                            command: [
                                "/root/server"
                            ],
                            name: name,
                            image: image.imageName,
                            imagePullPolicy: "Always",
                            ports: [
                                {
                                    containerPort: appPort,
                                    protocol: "TCP",
                                }
                            ],
                            env: [
                                {
                                    name: "GOR_DIR",
                                    value: "/tmp/logs"
                                },
                                {
                                    name: "BUCKET_NAME",
                                    value: input.shadowBucketName,
                                },
                                {
                                    name: "REGION",
                                    value: input.region,
                                },
                                {
                                    name: "PORT",
                                    value: `${appPort}`
                                }
                            ],
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
        return new k8s.core.v1.Service("query-server-shadow-svc", {
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
    });

    // Create kubernetes endpoint resolver, which configures emissary to resolve kubernetes endpoints.
    const endpointResolverName = 'shadow-query-server-endpoint-resolver';
    const resolver = new k8s.apiextensions.CustomResource("shadow-query-server-resolver", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "KubernetesEndpointResolver",
        metadata: {
            name: endpointResolverName,
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    // Setup ingress resources for query-server-shadow.
    const mapping = new k8s.apiextensions.CustomResource("query-server-shadow-mapping", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Mapping",
        metadata: {
            name: "data-server-shadow-mapping",
            labels: {
                "svc": "go-query-shadow",
            }
        },
        // emissary ingress requires the same spec for both original and shadow/canary mapping
        spec: {
            "hostname": "*",
            "prefix": "\/data\/(internal\/v1\/query|v1\/query|query)",
            "prefix_regex": true,
            "rewrite": "/query",
            "shadow": true,
            "service": `query-server-shadow:${appPort}`,
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
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const host = new k8s.apiextensions.CustomResource("query-server-shadow-host", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Host",
        metadata: {
            name: "query-server-shadow-host",
            labels: {
                "svc": "go-query-shadow",
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
                    "svc": "go-query-shadow",
                }
            },
            "requestPolicy": {
                "insecure": {
                    "action": "Route",
                }
            }
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    const output: outputType = {

    }
    return output
}
