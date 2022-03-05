import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";

const name = "countaggr"

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "kubernetes": "3.16.0",
    "docker": "v3.1.0",
    "aws": "v4.38.0"
}

export type inputType = {
    region: string,
    roleArn: string,
    kubeconfig: string,
    namespace: string,
}

export type outputType = {
    deployment: pulumi.Output<k8s.apps.v1.Deployment>,
}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {
        region: config.require(nameof<inputType>("region")),
        roleArn: config.require(nameof<inputType>("roleArn")),
        kubeconfig: config.require(nameof<inputType>("kubeconfig")),
        namespace: config.require(nameof<inputType>("namespace")),
    }
}

export const setup = async (input: inputType) => {
    const awsProvider = new aws.Provider("aggr-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    })

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository("countaggr-repo", {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    // Get registry info (creds and endpoint).
    const imageName = repo.repositoryUrl;
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

    // Build and publish the container image.
    const image = new docker.Image("countaggr-img", {
        build: {
            context: root,
            dockerfile: path.join(root, "dockerfiles/countaggr.dockerfile"),
            args: {
                "platform": "linux/amd64",
            },
        },
        imageName: imageName,
        registry: registryInfo,
    });

    const k8sProvider = new k8s.Provider("aggr-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const metricsPort = 2113;
    const appDep = image.imageName.apply(() => {
        return new k8s.apps.v1.Deployment("countaggr-deployment", {
            metadata: {
                name: "countaggr",
            },
            spec: {
                selector: { matchLabels: appLabels },
                replicas: 1,
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
                        containers: [{
                            name: name,
                            image: image.imageName,
                            imagePullPolicy: "Always",
                            command: [
                                "/root/countaggr",
                                "--metrics-port",
                                `${metricsPort}`,
                                "--dev=false"
                            ],
                            ports: [
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
                            ]
                        }],
                    },
                },
            },
        }, { provider: k8sProvider, deleteBeforeReplace: true });
    })

    const output: outputType = {
        deployment: appDep,
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
