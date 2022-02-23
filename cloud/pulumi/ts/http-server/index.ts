import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";


const namespace = "fennel"
const name = "http-server"

// TODO: use version from common library.
// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// TODO: move to common library module.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

export const plugins = {
    "kubernetes": "3.14.1",
    "docker": "v3.1.0",
    "aws": "v4.0.0"
}

export type inputType = {}

export type outputType = {}

const parseConfig = (): inputType => {
    const config = new pulumi.Config();
    return {}
}

export const setup = async (input: inputType) => {
    // Create a private ECR repository.
    const repo = new aws.ecr.Repository("http-server-repo", {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    });

    // Get registry info (creds and endpoint).
    const imageName = repo.repositoryUrl;
    const registryInfo = repo.registryId.apply(async id => {
        const credentials = await aws.ecr.getCredentials({ registryId: id });
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

    const baseImageName = image.baseImageName;
    const fullImageName = image.imageName;

    // Create a load balanced Kubernetes service using this image, and export its IP.
    const appLabels = { app: name };
    const metricsPort = 2112;
    const appDep = image.imageName.apply(() => {
        return new k8s.apps.v1.Deployment("http-server-deployment", {
            metadata: {
                name: "http-server",
                namespace: namespace,
            },
            spec: {
                selector: { matchLabels: appLabels },
                replicas: 1,
                template: {
                    metadata: {
                        labels: appLabels,
                        namespace: namespace,
                        annotations: {
                            "prometheus.io/scrape": "true",
                            "prometheus.io/port": metricsPort.toString(),
                        }
                    },
                    spec: {
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
                                    name: "CACHE_REPLICA",
                                    valueFrom: {
                                        secretKeyRef: {
                                            name: "cache-conf",
                                            key: "replica",
                                        }
                                    }
                                },
                            ]
                        },],
                    },
                },
            },
        }, { deleteBeforeReplace: true });
    })

    const appSvc = appDep.apply(() => {
        return new k8s.core.v1.Service("http-svc", {
            metadata: {
                labels: appLabels,
                name: name,
                namespace: namespace,
            },
            spec: {
                type: "ClusterIP",
                ports: [{ port: 2425, targetPort: 2425, protocol: "TCP" }],
                selector: appLabels,
            },
        }, { deleteBeforeReplace: true })
    }
    )
    const output: outputType = {}
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
