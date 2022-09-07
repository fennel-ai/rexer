import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws"
import * as docker from "@pulumi/docker";
import * as k8s from "@pulumi/kubernetes";
import {serviceEnvs} from "../tier-consts/consts";
import process from "process";
import childProcess from "child_process";
import path from "path";

const name = "counters-cleanup";

export const plugins = {
    "kubernetes": "v3.20.1",
    "docker": "v3.1.0",
    "aws": "v5.1.0"
}

const DEFAULT_USE_AMD64 = false

export type inputType = {
    region: string,
    roleArn: string,
    kubeconfig: string,
    namespace: string,
    tierId: number,
    useAmd64?: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    cronJob: k8s.batch.v1.CronJob,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const awsProvider = new aws.Provider("counter-cleanup-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    // Create a private ECR repository.
    const repo = new aws.ecr.Repository(`t-${input.tierId}-counter-cleanup-repo`, {
        imageScanningConfiguration: {
            scanOnPush: true
        },
        imageTagMutability: "MUTABLE"
    }, { provider: awsProvider });

    const repoPolicy = new aws.ecr.LifecyclePolicy(`t-${input.tierId}-counter-cleanup-repo-policy`, {
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

    let nodeSelector: Record<string, string> = {};
    const root = process.env["FENNEL_ROOT"]!
    // Get the (hash) commit id.
    // NOTE: This requires git to be installed and DOES NOT take local changes or commits into consideration.
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageName = repo.repositoryUrl.apply( imgName => {
        return `${imgName}:${hashId}`
    });

    let dockerfile, platform;
    if (input.useAmd64 || DEFAULT_USE_AMD64) {
        dockerfile = path.join(root, "dockerfiles/counters-cleanup.dockerfile")
        platform = "linux/amd64"
        nodeSelector["kubernetes.io/arch"] = "amd64"
    } else {
        dockerfile = path.join(root, "dockerfiles/counters-cleanup_arm64.dockerfile")
        platform = "linux/arm64"
        nodeSelector["kubernetes.io/arch"] = "arm64"
    }

    // NOTE: We do not set `CapacityType` for node selector configuration for counters cleanup servers since we
    // expect this to not run for more than 2 minutes - which is the duration before which the node is
    // notified and the pods are drained and removed. Also, kubernetes scheduler will not schedule the job
    // on a node which has already been signaled for termination

    // Build and publish the container image.
    const image = new docker.Image("counters-cleanup-img", {
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

    const k8sProvider = new k8s.Provider("counters-cleanup-k8s-provider", {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    });

    const appLabels = { app: name };

    const cronJob = image.imageName.apply(() => {
        return new k8s.batch.v1.CronJob("counters-cleanup-cronjob", {
            metadata: {
                name: "counters-cleanup",
            },
            spec: {
                jobTemplate: {
                    metadata: {
                        labels: appLabels,
                    },
                    spec: {
                        template: {
                            metadata: {
                                annotations: {
                                    // disable inject linkerd proxy for the cleanup service
                                    //
                                    // cleanup-service is a background job and does not accept open traffic, hence
                                    // does not need linkerd-proxy to be running along side it
                                    "linkerd.io/inject": "disabled",
                                },
                                labels: appLabels,
                            },
                            spec: {
                                containers: [{
                                    name: name,
                                    command: [
                                        "/root/cleanup",
                                        "--dev=false",
                                    ],
                                    image: image.imageName,
                                    // TODO(Mohit): Consider trimming the list down here. Counters clean up
                                    // only requires RDS and MemoryDB addresses to fetch aggregates which are
                                    // inactive and delete the corresponding counters.
                                    env: serviceEnvs,
                                }],
                                // This has to be either `OnFailure` or `Never`
                                restartPolicy: "OnFailure",
                                nodeSelector: nodeSelector,
                            }
                        }
                    }
                },
                // At minute 0 past every 2nd hour; NOTE: this can be called explicitly as well using kubectl
                schedule: "0 */2 * * *",
                // it is okay to run concurrent jobs
                concurrencyPolicy: "Allow",
                // keep the jobs in both states around for debugging and investigations
                failedJobsHistoryLimit: 1,
                successfulJobsHistoryLimit: 1,
            }
        }, {provider: k8sProvider, deleteBeforeReplace: true});
    });

    const output = pulumi.output({
        cronJob: cronJob,
    })
    return output
}
