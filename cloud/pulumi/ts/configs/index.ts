import * as k8s from "@pulumi/kubernetes";
import * as pulumi from "@pulumi/pulumi";

const config = new pulumi.Config();

const rc = new k8s.core.v1.Secret("redis-config", {
    stringData: config.requireSecretObject("redis-conf"),
    metadata: {
        namespace: "fennel",
        name: "redis-conf",
    },
}, { deleteBeforeReplace: true })

const kafkaCreds = new k8s.core.v1.Secret("kafka-config", {
    stringData: config.requireSecretObject("kafka-conf"),
    metadata: {
        name: "kafka-conf",
        namespace: "fennel",
    }
}, { deleteBeforeReplace: true })

const pscreds = new k8s.core.v1.Secret("db-config", {
    stringData: config.requireSecretObject("db-conf"),
    metadata: {
        namespace: "fennel",
        name: "mysql-conf",
    }
}, { deleteBeforeReplace: true })

const tierid = new k8s.core.v1.ConfigMap("tier-conf", {
    data: config.requireObject("tier-conf"),
    metadata: {
        namespace: "fennel",
        name: "tier-conf",
    }
}, { deleteBeforeReplace: true })
