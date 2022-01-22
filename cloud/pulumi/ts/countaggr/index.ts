import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as path from "path";
import * as k8s from "@pulumi/kubernetes";

const namespace = "fennel"
const name = "countaggr"

// Create a private ECR repository.
const repo = new aws.ecr.Repository("countaggr-repo", {
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

// Export the base and specific version image name.
export const baseImageName = image.baseImageName;
export const fullImageName = image.imageName;

// Create a load balanced Kubernetes service using this image, and export its IP.
const appLabels = { app: name };
const appDep = image.imageName.apply(() => {
    return new k8s.apps.v1.Deployment("countaggr-deployment", {
        metadata: {
            name: "countaggr",
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
                    }],
                },
            },
        },
    }, { deleteBeforeReplace: true });
})

export const deployment = appDep.id
