import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";
import * as postgresql from "@pulumi/postgresql";
import {POSTGRESQL_PASSWORD, POSTGRESQL_USERNAME} from "../tier-consts/consts";


export const plugins = {
    "kubernetes": "v3.16.0",
    "postgresql": "v3.4.0",
    "aws": "v4.38.1",
}

export type inputType = {
    region: string,
    roleArn: string,
    tierId: number,
    namespace: string,
    dbEndpoint: string,
    dbPort: number,
    kubeconfig: string,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    logBucket: string,
    airbyteDbName: string,
}

export const setup = async (input: inputType): Promise<outputType> => {
    // providers
    const provider = new aws.Provider(`t-${input.tierId}-airbyte-provider`, {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });
    const k8sProvider = new k8s.Provider(`t-${input.tierId}-airbyte-k8s-provider`, {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    });

    // create database in the postgres endpoint
    const databaseName = `t_${input.tierId}_airbytedb`;
    const postgresProvider = new postgresql.Provider("airbyte-postgresql-provider", {
        host: input.dbEndpoint,
        port: input.dbPort,
        superuser: true,
        databaseUsername: POSTGRESQL_USERNAME,
        username: POSTGRESQL_USERNAME,
        password: POSTGRESQL_PASSWORD,
    }, { provider: provider });
    const db = new postgresql.Database(databaseName, {
        name: databaseName,
    }, { provider: postgresProvider, protect: input.protect });

    // create s3 bucket
    const bucketName = `t-${input.tierId}-airbyte-logs`
    const bucket = new aws.s3.Bucket("airbyte-log-store", {
        acl: "private",
        bucket: bucketName,
        // delete all the objects os that bucket can be deleted without error
        forceDestroy: true,
    }, { provider: provider, protect: input.protect });

    // create an AWS user account to authenticate airbyte workers to write logs to S3 bucket
    const user = new aws.iam.User("airbyte-user", {
        name: `t-${input.tierId}-airbyte-user`,
        // set path to differentiate this user from the rest of human users
        path: "/airbyte/",
        tags: {
            "managed_by": "fennel.ai",
            "tier": `t-${input.tierId}`,
        }
    }, { provider, dependsOn: bucket });

    // fetch access keys
    const userAccessKey = new aws.iam.AccessKey("airbyte-user-access-key", {
        user: user.name
    }, { provider });

    const userPolicy = new aws.iam.UserPolicy("airbyte-user-policy", {
        user: user.name,
        policy: JSON.stringify({
            Version: "2012-10-17",
            Statement: [
                {
                    Effect: "Allow",
                    Action: [
                        "s3:ListBucket",
                        "s3:GetBucketLocation",
                    ],
                    Resource: [
                        `arn:aws:s3:::${bucketName}`,
                    ]
                },
                {
                    Effect: "Allow",
                    Action: [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject"
                    ],
                    Resource: [
                        `arn:aws:s3:::${bucketName}/*`,
                    ]
                },
            ]
        }),
    }, { provider });

    // create a secret for the DB password
    const secretName = `airbyte-password-secret`;
    const airbyteSecret = new k8s.core.v1.Secret("airbyte-password-config", {
        stringData: {
            "password": POSTGRESQL_PASSWORD,
            "accessKey": userAccessKey.id,
            "secretKey": userAccessKey.secret,
        },
        metadata: {
            name: secretName,
        },
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    // setup airbyte instance

    // TODO(mohit): Airbyte currently does not officially publish the helm chart, however they have implemented one
    // here - https://github.com/airbytehq/airbyte/tree/master/charts/airbyte
    //
    // To make progress on our end, we have forked this helm chart and published it here -
    //  https://github.com/fennel-ai/public/tree/main/helm-charts/airbyte
    //
    // This is also mentioned in the Airbyte Issue - https://github.com/airbytehq/airbyte/issues/1868#issuecomment-1025952077
    const imageTag = "0.39.1-alpha";
    const airbyteRelease = new k8s.helm.v3.Release("airbyte", {
        repositoryOpts: {
            "repo": "https://fennel-ai.github.io/public/helm-charts/airbyte/",
        },
        chart: "airbyte",
        values: {
            "version": imageTag,

            // disable injecting linkerd for all the pods which are spun up - by default they are injected for all
            // pods which are deployed in the tier namespace

            // Airbyte services are multi-arch compatible except for webapp and the external iamges they use for:
            // i) pod sweeper (this is kubectl) ii) temporal which is the orchestrator - https://hub.docker.com/r/temporalio/auto-setup/tags
            //
            // Temporal do publish multi-arch compatible images but the version pinned on airbyte is old (~1 year)
            // and did not have support back then
            //
            // We will explicitly schedule them amd64 machines
            "webapp": {
                "nodeSelector": {
                    "kubernetes.io/arch": "amd64",
                },
                "image": {
                    "tag": imageTag,
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },

            "podSweeper": {
                "nodeSelector": {
                    "kubernetes.io/arch": "amd64",
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },

            "temporal": {
                "nodeSelector": {
                    "kubernetes.io/arch": "amd64",
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },

            "server": {
                "image": {
                    "tag": imageTag,
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },
            "bootloader": {
                "image": {
                    "tag": imageTag,
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },

            // NOTE: We should configure rest of the Airbyte resources as per recommendations from:
            // https://docs.airbyte.com/deploying-airbyte/on-kubernetes#production-airbyte-on-kubernetes

            // NOTE: Currently we do not set resource limits for core container pods - defaults or undefined
            // requests and limits are fine for now. We can increase/restrict them once we notice delays or pods
            // crashing

            // NOTE: Connector are run as pods with no resource requests and limits. We may have to configure them
            // by setting environment variables `JOB_MAIN_CONTAINER_CPU_REQUEST` ... in the `worker` pod
            //
            // See - https://github.com/airbytehq/airbyte/search?q=JOB_MAIN_CONTAINER_MEMORY_REQUEST

            // increase number of workers to increase job parallelism
            "worker": {
                // Consider increasing as the number of sources increase
                "replicaCount": 2,
                "image": {
                    "tag": imageTag,
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },

            // we need to set the `JOB_KUBE_NODE_SELECTORS` env var to schedule the workers on amd64 workers since
            // the worker pods are spun up with airbyte data_integration container (which is multi-arch compatible), but
            // the other containers (e.g. `alpine/socat:1.7.4.3-r0`) can only run on amd64 machines
            "jobs": {
                "kube": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                    },
                    "annotations": {
                        // disable injecting linkerd for the data_integration jobs
                        "linkerd.io/inject": "disabled",
                    }
                }
            },

            // enable s3 logging
            "logs": {
                "accessKey": {
                    "existingSecret": secretName,
                    "existingSecretKey": "accessKey",
                },
                "secretKey": {
                    "existingSecret": secretName,
                    "existingSecretKey": "secretKey",
                },
                // disable minio logging which is configured by default
                "minio": {
                    "enabled": false,
                },
                "s3": {
                    "enabled": true,
                    "bucket": bucketName,
                    "bucketRegion": input.region,
                }
            },

            // configure external DB where jobs and configurations are persisted.
            // See - https://docs.airbyte.com/operator-guides/configuring-airbyte-db/
            //
            // Disable local postgresql
            "postgresql": {
                "enabled": false,
            },
            "externalDatabase": {
                "host": input.dbEndpoint,
                "user": POSTGRESQL_USERNAME,
                "database": databaseName,
                "port": input.dbPort,

                // these should match the secret `airbyteSecret` created above
                "existingSecret": secretName,
                "existingSecretPasswordKey": "password",
            }
        },
    }, { provider: k8sProvider, dependsOn: [airbyteSecret] });

    return {
        logBucket: bucketName,
        airbyteDbName: databaseName,
    };
}
