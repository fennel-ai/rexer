import * as docker from "@pulumi/docker";
import * as aws from "@pulumi/aws"
import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";
import * as path from "path";
import * as process from "process";
import * as childProcess from "child_process";
import {serviceEnvs} from "../tier-consts/consts";

const name = "countaggr"

export const plugins = {
    "kubernetes": "v3.18.0",
    "docker": "v3.1.0",
    "aws": "v5.1.0"
}

const DEFAULT_ENFORCE_SEPARATION = false;

export type inputType = {
    region: string,
    roleArn: string,
    kubeconfig: string,
    namespace: string,
    tierId: number,
    enforceServiceIsolation?: boolean,
    nodeLabels?: Record<string, string>,
    httpServerAppLabels: {[key: string]: string},
}

export type outputType = {
    deployment: pulumi.Output<k8s.apps.v1.Deployment>,
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
    const repo = new aws.ecr.Repository(`t-${input.tierId}-countaggr-repo`, {
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
    const imageName = repo.repositoryUrl.apply( imgName => {
        return `${imgName}:${hashId}`
    })

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

    // define affinity for countaggr service based on input configuration
    let affinity: k8s.types.input.core.v1.Affinity = {};
    if (input.enforceServiceIsolation || DEFAULT_ENFORCE_SEPARATION) {
        affinity.podAntiAffinity = {
            // NOTE: On scheduling, if the following requires are not met by the node, pod is
            // not going to be scheduled in it. However, if the requirements specified by this
            // field cease to be met at some point during pod execution (e.g. update), this
            // pod may or may not get eventually evicted.
            requiredDuringSchedulingIgnoredDuringExecution: [
                // the following requirements MUST match i.e. intersection of the nodes qualified
                // is a potential node

                // Avoid scheduling the pod onto a node that is in the same host as one
                // or more pods with the label `app:http-server`.
                {
                    topologyKey: "kubernetes.io/hostname",
                    labelSelector: {
                        matchLabels: input.httpServerAppLabels,
                    }
                    // namespaces: [] -> default, just search in this pod's namespace
                },
            ],
        };
    }

    // if node labels are specified, create an affinity for the pod towards that node (or set of nodes)
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
                        affinity: affinity,
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
                            env: serviceEnvs,
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
