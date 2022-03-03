import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as pulumi from "@pulumi/pulumi"
import * as path from "path";
import * as k8s from "@pulumi/kubernetes";
import { rootPulumiStackTypeName } from "@pulumi/pulumi/runtime";

const namespace = "fennel"
const name = "bridge"

// Create a private ECR repository.
const repo = new aws.ecr.Repository("bridge-repo", {
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
const image = new docker.Image("bridge-img", {
    build: {
        context: root,
        dockerfile: path.join(root, "dockerfiles/bridge.dockerfile"),
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

const config = new pulumi.Config();

// Create a load balanced Kubernetes service using this image, and export its IP.
const appLabels = { app: name };
const appDep = image.imageName.apply(() => {
    return new k8s.apps.v1.Deployment("bridge-deployment", {
        metadata: {
            name: "bridge",
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
                                containerPort: 2475,
                                protocol: "TCP",
                            },
                        ],
                        command: [
                            "/root/bridge"
                        ],
                        env: [
                            {
                                name: "MOTHERSHIP_ID",
                                value: config.require("mothershipId"),
                            },
                            {
                                name: "MOTHERSHIP_MYSQL_ADDRESS",
                                value: config.require("dbhost"),
                            },
                            {
                                name: "MOTHERSHIP_MYSQL_USERNAME",
                                value: config.require("dbuser"),
                            },
                            {
                                name: "MOTHERSHIP_MYSQL_PASSWORD",
                                value: config.requireSecret("dbpassword"),
                            },
                            {
                                name: "MOTHERSHIP_MYSQL_DBNAME",
                                value: config.require("db"),
                            },
                            {
                                name: "BRIDGE_PORT",
                                value: "2475",
                            },
                        ]
                    }],
                },
            },
        },
    }, { deleteBeforeReplace: true });
})

const appSvc = appDep.apply(() => {
    return new k8s.core.v1.Service("control-svc", {
        metadata: {
            labels: appLabels,
            name: name,
            namespace: namespace,
        },
        spec: {
            type: "ClusterIP",
            ports: [{ port: 2475, targetPort: 2475, protocol: "TCP" }],
            selector: appLabels,
        },
    }, { deleteBeforeReplace: true })
}
)

export const svc = appSvc.metadata.name
