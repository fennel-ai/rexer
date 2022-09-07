import * as k8s from "@pulumi/kubernetes";
import * as aws from "@pulumi/aws";
import * as postgresql from "@pulumi/postgresql";
import {POSTGRESQL_PASSWORD, POSTGRESQL_USERNAME} from "../tier-consts/consts";

export const plugins = {
    "kubernetes": "v3.20.1",
    "postgresql": "v3.4.0",
}

export type inputType = {
    roleArn: string,
    region: string,
    tierId: number,
    namespace: string,
    unleashDbEndpoint: string,
    unleashDbPort: number,
    kubeconfig: string,
    protect: boolean,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    unleashEndpoint: string,
}

export const setup = async (input: inputType): Promise<outputType> => {
    const provider = new aws.Provider(`t-${input.tierId}-unleash-db-provider`, {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const k8sProvider = new k8s.Provider(`t-${input.tierId}-unleash-k8s-provider`, {
        kubeconfig: input.kubeconfig,
        namespace: input.namespace,
    })

    const databaseName = `t_${input.tierId}_unleashdb`;
    const postgresProvider = new postgresql.Provider("postgresql-provider", {
        host: input.unleashDbEndpoint,
        port: input.unleashDbPort,
        superuser: true,
        databaseUsername: POSTGRESQL_USERNAME,
        username: POSTGRESQL_USERNAME,
        password: POSTGRESQL_PASSWORD,
    }, { provider: provider });
    const db = new postgresql.Database(databaseName, {
        name: databaseName,
    }, { provider: postgresProvider, protect: input.protect, dependsOn: postgresProvider });

    // setup unleash with:
    //  1. disable API tokens - it is not possible to create one automatically; we disable API tokens and enforce
    //      the isolation from the setup perspective - i.e. each tier will create one instance and use the endpoint
    //      as returned from the setup.
    //  2. disable postgres - this will use a postgres instance running outside of the cluster, removing requirement of
    //      maintaining the state of unleash if the node/pod goes down (or has any issues on the volumes attached to the
    //      node).
    //  3. disable authentication - this service is deployed in the EKS cluster which is already behind private
    //      subnets and have coarser security groups defined.
    const releaseName = `t-${input.tierId}-unleash`;
    const containerPort = 4242;
    const unleash = new k8s.helm.v3.Release(releaseName, {
        name: releaseName,
        repositoryOpts: {
            "repo": "https://docs.getunleash.io/helm-charts",
        },
        chart: "unleash",
        version: "2.6.1",
        values: {
            "configMaps": {
                "index.js": {
                    "mountPath": "/unleash/index.js",
                    "content": `
                        'use strict';
                        const unleash = require('unleash-server');

                        unleash.start({
                            authentication: {
                                enableApiToken: false,
                                type: 'none',
                            }
                        });
                    `,
                }
            },
            // by default uses image tagged 4.4.4
            "nodeSelector": {
                // we should schedule all components of Unleash on ON_DEMAND and X86 instances
                "eks.amazonaws.com/capacityType": "ON_DEMAND",
                "kubernetes.io/arch": "amd64",
            },
            "fullnameOverride": `t-${input.tierId}-unleash`,
            "postgresql": {
                "enabled": false,
            },
            "dbConfig": {
                "database": databaseName,
                "host": input.unleashDbEndpoint,
                "port": input.unleashDbPort,
                "user": POSTGRESQL_USERNAME,
                "pass": POSTGRESQL_PASSWORD,
            },
            "containerPort": containerPort,
        }
    // replace on any changes. The features are persisted in the postgres DB so this should be work as intended
    }, { provider: k8sProvider, replaceOnChanges: ["*"], deleteBeforeReplace: true });

    return {
        unleashEndpoint: `http://${releaseName}.${input.namespace}:${containerPort}/api/`,
    }
}
