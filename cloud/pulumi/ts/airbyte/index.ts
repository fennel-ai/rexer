import * as pulumi from "@pulumi/pulumi";
import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";
import * as postgresql from "@pulumi/postgresql";
import {POSTGRESQL_PASSWORD, POSTGRESQL_USERNAME} from "../tier-consts/consts";


const DEFAULT_AIRBYTE_SERVER_PUBLIC = false;

export const plugins = {
    "kubernetes": "v3.20.1",
    "postgresql": "v3.4.0",
    "aws": "v5.0.0",
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
    publicServer?: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    logBucket: string,
    dbName: string,
    endpoint: string,
}

// TODO(mohit, aditya): Replace this with Fennel's official GCP Account
const FENNEL_GCP_PROJECT_ID = 'gold-cocoa-356105';
const FENNEL_GCP_CREDENTIALS_JSON = "{\"type\": \"service_account\", \"project_id\": \"gold-cocoa-356105\", \"private_key_id\": \"d7fa128cbbcc6e5a1d1615ed9faafc621829810d\"," +
    "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCXXKsnDnAsDyhx\\nUdJjiZ0dBx57dH6dQWswE5puMIG5uO7uFRyA7gOB2i9eiCLDPhz7T33QhQaG+7aK\\n+vdtpBLMBImHG98w7WAWY25/8vInBaSbTo7fFvc5r/9FgODqP9M5rVTCBReIscbC\\nrnwAnscDB/kZd5xLNYmXYWU+YjbMJQfCoGc8Js6EBjqheTnWk5ZBGfofwbOFQZVQ\\nQY00IWkicW9I/vidap3VaCdsRC7UZDxxMriu+wUSL+d2ZaCgH9RD/05mGz2uMeX7\\npTe/az3rD83meJpUPbKHYERFGGHJyav05occsLoFvCTl1FIfP9FbABZTNDe+Bwxl\\nkcTY+UHfAgMBAAECggEAAOWzPAmJX7F9T2KpSR7FOClVJG013O/I12GeXj3aXwP6\\nIp4sa5U9nxTwh/JtplOlb1XyzHwlZEJ0vBEty1AYLm5udEcVhSA7HBbdzlNd3R5a\\n8fK+xRLJR2XEMSDI9IqJUYO2B2ppT82h/IB1SrmmO13eO6jqW8XG+YdBxuNlKMOj\\nGRwNurfmDDGvXJvNZAGUfEjoUPvz5iefJqBSkxm29WkgmN6vf8gOTNgNbFfK2AE/\\nRdLu5nRiynFvDQf9BsYKF/WNXaRWo/WKT8X6c0HWvRn2CBEXhV9pNGXiFhi/P8y9\\n0UwpzMvKN4Ti7p9YAPyTjIEj2V7e9sLHEZe5RYzZpQKBgQDH3gjIGN6VRkQjxaeo\\nRYhCIQisipWdqJW93LFH7bx8o9qO86TnbEnIdPvxnptLOhqWrC9AP8UtjlZJRCyt\\ng+v19RKMWxnwye6cFq1AcwM0rkB9OydIxKVF+MZ0jZU1k6zMDAfg2obXFGZXIzQL\\nNKX1fR2GEye2rqmlNelUhVFfswKBgQDB3ziCn70C0TVeKSLy4xopDbAKxW1l2lnu\\nWeHY7eevdaMPx34Q4lkeleTzUaLV5gGjvvKFBzzts84XIrYcc3goHGtkA/ObHCdv\\nP2HopYC/zfm6va/Rv75DK18s+Ic6WQsBfXlv7Byy9zKVFc0AhWZwE3bsKnz2mEQl\\nYI5/9gFfJQKBgD2YMrKf33C3f+ZaUonsK8rdbVPnPaahvswNSGE3ZeAvivqFIavk\\nVnS9gKt8yrULSghnNgSh4n1goTzhErfCsSRSi43PwZXQVYWrA2eaSkGg9eTiJwAp\\nAhonSdm/jF0/joAvsPndvrJn6gYupipR5ldaYI/iNVn6R/PPQoI2t9Y7AoGBAKXn\\nswE9V08Y3xWkGE9H/vQQzYx6JMMblwf8jOPJuxGQlqkDK6OhP2iIF3QNcU6gVNje\\np8UlS4OS8hMkVjmEqteQcmoVY5th/XEbCVtAfiwlRMcEWnghIN10OS9PwtEwr9Vn\\nncskf+660eN404TVo7LXRVaWiXexF+fweCGS0NutAoGAL49ERMHrwihF4K0ERSwR\\ndIHf+oemHduCmzsd8wdUGQgyrI3OBky4hwf1wdX+8yPxOfRK/fZSM6otm8YamOlo\\nwTJY5+OgmyyzAtfmbdA71zaNbE6V2kAkcoWxjCDGqFrX5LAMo590RP5FpVmND3AS\\n0Ni9HFmV+PwPdx3FyL2+g60=\\n-----END PRIVATE KEY-----\\n\"," +
    "  \"client_email\": \"fennel@gold-cocoa-356105.iam.gserviceaccount.com\"," +
    "  \"client_id\": \"112871096554223481842\"," +
    "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\"," +
    "  \"token_uri\": \"https://oauth2.googleapis.com/token\"," +
    "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\"," +
    "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/fennel%40gold-cocoa-356105.iam.gserviceaccount.com\"" +
    "}";

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
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

    // create a secret to store GCP information
    const gcpSecretName = `airbyte-gcp-secret`;
    const gcpAirbyteSecret = new k8s.core.v1.Secret("airbyte-gcp-password-config", {
        stringData: {
            "projectId": FENNEL_GCP_PROJECT_ID,
            "credentialsJson": FENNEL_GCP_CREDENTIALS_JSON,
        },
        metadata: {
            name: gcpSecretName,
        }
    }, { provider: k8sProvider, deleteBeforeReplace: true });

    // setup airbyte instance

    let serverServiceType;
    if (input.publicServer || DEFAULT_AIRBYTE_SERVER_PUBLIC) {
        serverServiceType = "LoadBalancer";
    } else {
        // by default this is of type `ClusterIP`
        serverServiceType = "ClusterIP";
    }

    // TODO(mohit): Airbyte currently does not officially publish the helm chart, however they have implemented one
    // here - https://github.com/airbytehq/airbyte/tree/master/charts/airbyte
    //
    // To make progress on our end, we have forked this helm chart and published it here -
    //  https://github.com/fennel-ai/public/tree/main/helm-charts/airbyte
    //
    // This is also mentioned in the Airbyte Issue - https://github.com/airbytehq/airbyte/issues/1868#issuecomment-1025952077
    const imageTag = "0.39.1-alpha";
    const serverPort = 8001;
    const airbyteRelease = new k8s.helm.v3.Release("airbyte", {
        repositoryOpts: {
            "repo": "https://fennel-ai.github.io/public/helm-charts/airbyte/",
        },
        chart: "airbyte",
        version: "0.3.9",
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
                    // we should schedule all components of Airbyte on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
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
                    // we should schedule all components of Airbyte on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
            },

            "temporal": {
                "nodeSelector": {
                    "kubernetes.io/arch": "amd64",
                    // we should schedule all components of Airbyte on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
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
                "nodeSelector": {
                    // we should schedule all components of Airbyte on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                },
                // service type for the airbyte server
                "service": {
                    "type": serverServiceType,
                    "port": serverPort,
                },
                "extraEnv": [
                    {
                        "name": "SECRET_PERSISTENCE",
                        "value": "GOOGLE_SECRET_MANAGER"
                    },
                    {
                        "name": "SECRET_STORE_GCP_PROJECT_ID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "key": "projectId",
                                "name": gcpSecretName,
                            }
                        }
                    },
                    {
                        "name": "SECRET_STORE_GCP_CREDENTIALS",
                        "valueFrom": {
                            "secretKeyRef": {
                                "key": "credentialsJson",
                                "name": gcpSecretName,
                            }
                        }
                    }
                ]
            },
            "bootloader": {
                "image": {
                    "tag": imageTag,
                },
                "podAnnotations": {
                    "linkerd.io/inject": "disabled",
                },
                "nodeSelector": {
                    // we should schedule all components of Airbyte on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                }
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
                "nodeSelector": {
                    // we should schedule all components of Airbyte on ON_DEMAND instances
                    "eks.amazonaws.com/capacityType": "ON_DEMAND",
                },
                "extraEnv": [
                    {
                        "name": "SECRET_PERSISTENCE",
                        "value": "GOOGLE_SECRET_MANAGER"
                    },
                    {
                        "name": "SECRET_STORE_GCP_PROJECT_ID",
                        "valueFrom": {
                            "secretKeyRef": {
                                "key": "projectId",
                                "name": gcpSecretName,
                            }
                        }
                    },
                    {
                        "name": "SECRET_STORE_GCP_CREDENTIALS",
                        "valueFrom": {
                            "secretKeyRef": {
                                "key": "credentialsJson",
                                "name": gcpSecretName,
                            }
                        }
                    }
                ]
            },

            // we need to set the `JOB_KUBE_NODE_SELECTORS` env var to schedule the workers on amd64 workers since
            // the worker pods are spun up with airbyte connector container (which is multi-arch compatible), but
            // the other containers (e.g. `alpine/socat:1.7.4.3-r0`) can only run on amd64 machines
            "jobs": {
                "kube": {
                    "nodeSelector": {
                        "kubernetes.io/arch": "amd64",
                        // we should schedule all components of Airbyte on ON_DEMAND instances
                        "eks.amazonaws.com/capacityType": "ON_DEMAND",
                    },
                    "annotations": {
                        // disable injecting linkerd for the connector jobs
                        "linkerd.io/inject": "disabled",
                    }
                },

                // set resources for the jobs which reads and writes data from the source to the sink
                //
                // TODO(mohit): This should be made specific to the tiers, based on their resource requirements
                // (amount of data we might potentially read on every sync)
                "resources": {
                    "requests": {
                        "cpu": "6500m",
                        "memory": "12Gi",
                    },
                    "limits": {
                        "cpu": "7500m",
                        "memory": "15Gi",
                    },
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

    // append `-server` to get the service name of the airbyte server up and running
    const airbyteServiceEndpoint = airbyteRelease.name.apply(releaseName => {
        return `http://${releaseName}-server.${input.namespace}:${serverPort}/api`;
    });

    return pulumi.output({
        logBucket: bucketName,
        dbName: databaseName,
        endpoint: airbyteServiceEndpoint,
    });
}
