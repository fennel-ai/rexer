import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as path from "path";
import * as k8s from "@pulumi/kubernetes";

const namespace = "fennel"
const name = "http-server"

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

// Export the base and specific version image name.
export const baseImageName = image.baseImageName;
export const fullImageName = image.imageName;

// Create a load balanced Kubernetes service using this image, and export its IP.
const appLabels = { app: name };
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
                },
                spec: {
                    containers: [{
                        name: name,
                        image: image.imageName,
                        imagePullPolicy: "Always",
                        ports: [
                            {
                                containerPort: 2425,
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

export const svc = appSvc.metadata.name