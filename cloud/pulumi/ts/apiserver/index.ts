import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";
import * as childProcess from "child_process";

const name = "api-server"

export const plugins = {
    "kubernetes": "v3.18.0",
    "docker": "v3.1.0",
    "aws": "v5.1.0"
}

const DEFAULT_REPLICAS = 1
const DEFAULT_FORCE_REPLICA_ISOLATION = false

export type inputType = {
    region: string,
    roleArn: string,
    kubeconfig: string,
    namespace: string,
    tierId: number,
    replicas?: number,
    enforceReplicaIsolation?: boolean,
    storageclass?: string
}

export type outputType = {
    appLabels: { [key: string]: string },
    svc: pulumi.Output<k8s.core.v1.Service>,
}

export const setup = async (input: inputType) => {
    const awsProvider = new aws.Provider("api-server-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`t-${input.tierId}-api-server-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
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
    const imageName = repo.repositoryUrl.apply(imgName => {
        return `${imgName}:${hashId}`
    })

    // Build and publish the container image.
    const image = new docker.Image("api-server-img", {
        build: {
            context: root,
            dockerfile: path.join(root, "dockerfiles/apiserver.dockerfile"),
            args: {
                "platform": "linux/amd64",
            },
        },
        imageName: imageName,
        registry: registryInfo,
    });

    const k8sProvider = new k8s.Provider("api-server-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const metricsPort = 2112;

    const forceReplicaIsolation = input.enforceReplicaIsolation || DEFAULT_FORCE_REPLICA_ISOLATION;
    let whenUnsatisfiable = "ScheduleAnyway";
    if (forceReplicaIsolation) {
        whenUnsatisfiable = "DoNotSchedule";
    }

    if (input.storageclass === undefined) {
        console.log('storageClass is undefined for API server, will use default storage class for persistent volume.')
    }

    const appStatefulset = image.imageName.apply(() => {
        return new k8s.apps.v1.StatefulSet("api-server-statefulset", {
            metadata: {
                name: "api-server",
                labels: appLabels,
            },
            spec: {
                serviceName: "api-server",
                selector: { matchLabels: appLabels },
                // NOTE: If changing number replicas, please take: size and desired capacity of the nodegroup,
                // affinity b/w http-server and countaggr services into consideration.
                //
                // NOTE: If changing number replicas, please take `topologySpreadConstraints`
                // into consideration which schedules replicas on different nodes.
                replicas: input.replicas || DEFAULT_REPLICAS,
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
                        containers: [
                            {
                                command: [
                                    "/root/server",
                                    "--metrics-port",
                                    "2112",
                                    "--dev=false",
                                    "--badger_dir=/blaze"
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
                                env: [
                                    {
                                        name: "KAFKA_SERVER_ADDRESS",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "kafka-conf",
                                                key: "server",
                                            }
                                        }
                                    },
                                    {
                                        name: "KAFKA_USERNAME",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "kafka-conf",
                                                key: "username",
                                            }
                                        }
                                    },
                                    {
                                        name: "KAFKA_PASSWORD",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "kafka-conf",
                                                key: "password",
                                            }
                                        }
                                    },
                                    {
                                        name: "REDIS_SERVER_ADDRESS",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "redis-conf",
                                                key: "addr",
                                            }
                                        }
                                    },
                                    {
                                        name: "MYSQL_SERVER_ADDRESS",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "mysql-conf",
                                                key: "host",
                                            }
                                        }
                                    },
                                    {
                                        name: "MYSQL_DATABASE_NAME",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "mysql-conf",
                                                key: "db",
                                            }
                                        }
                                    },
                                    {
                                        name: "MYSQL_USERNAME",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "mysql-conf",
                                                key: "username",
                                            }
                                        }
                                    },
                                    {
                                        name: "MYSQL_PASSWORD",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "mysql-conf",
                                                key: "password",
                                            }
                                        }
                                    },
                                    {
                                        name: "TIER_ID",
                                        valueFrom: {
                                            configMapKeyRef: {
                                                name: "tier-conf",
                                                key: "tier_id",
                                            }
                                        }
                                    },
                                    {
                                        name: "CACHE_PRIMARY",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "cache-conf",
                                                key: "primary",
                                            }
                                        }
                                    },
                                    {
                                        name: "AWS_REGION",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "model-serving-conf",
                                                key: "region",
                                            }
                                        }
                                    },
                                    {
                                        name: "SAGEMAKER_EXECUTION_ROLE",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "model-serving-conf",
                                                key: "executionRole",
                                            }
                                        }
                                    },
                                    {
                                        name: "PRIVATE_SUBNETS",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "model-serving-conf",
                                                key: "privateSubnets",
                                            }
                                        }
                                    },
                                    {
                                        name: "SAGEMAKER_SECURITY_GROUP",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "model-serving-conf",
                                                key: "securityGroup",
                                            }
                                        }
                                    },
                                    {
                                        name: "MODEL_STORE_S3_BUCKET",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "model-serving-conf",
                                                key: "modelStoreBucket",
                                            }
                                        }
                                    },
                                    {
                                        name: "MODEL_STORE_ENDPOINT",
                                        valueFrom: {
                                            secretKeyRef: {
                                                name: "model-serving-conf",
                                                key: "modelStoreEndpoint",
                                            }
                                        }
                                    }
                                ],
                                volumeMounts: [
                                    {
                                        name: "badgerdb",
                                        mountPath: "/blaze",
                                    }
                                ]
                            },
                        ],
                    },
                },
                volumeClaimTemplates: [
                    {
                        metadata: {
                            name: "badgerdb",
                        },
                        spec: {
                            accessModes: ["ReadWriteOnce"],
                            // if the storage class is undefined, default storage class is used by the PVC.
                            storageClassName: input.storageclass,
                            resources: {
                                requests: {
                                    storage: "100Gi",
                                },
                            }
                        }
                    }
                ]
            },
        }, { provider: k8sProvider, deleteBeforeReplace: true });
    })

    const appSvc = appStatefulset.apply(() => {
        return new k8s.core.v1.Service("api-server-svc", {
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

    // Setup ingress resources for api-server
    const mapping = new k8s.apiextensions.CustomResource("apiserver-mapping", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Mapping",
        metadata: {
            name: "apiserver-mapping",
            labels: {
                "svc": "go-api-server",
            }
        },
        spec: {
            "hostname": "*",
            "prefix": "/api/",
            "service": "api-server:2425",
            "timeout_ms": 30000,
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const host = new k8s.apiextensions.CustomResource("apiserver-host", {
        apiVersion: "getambassador.io/v3alpha1",
        kind: "Host",
        metadata: {
            name: "apiserver-host",
            labels: {
                "svc": "go-api-server",
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
                    "svc": "go-api-server",
                }
            },
            "requestPolicy": {
                "insecure": {
                    "action": "Route",
                }
            }
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true })

    const output: outputType = {
        appLabels: appLabels,
        svc: appSvc,
    }
    return output
}
