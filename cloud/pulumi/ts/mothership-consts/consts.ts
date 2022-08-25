export const serviceEnvs = [
    {
        name: "MOTHERSHIP_MYSQL_ADDRESS",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "host",
            }
        }
    },
    {
        name: "MOTHERSHIP_MYSQL_DBNAME",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "db",
            }
        }
    },
    {
        name: "MOTHERSHIP_MYSQL_USERNAME",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "username",
            }
        }
    },
    {
        name: "MOTHERSHIP_MYSQL_PASSWORD",
        valueFrom: {
            secretKeyRef: {
                name: "mysql-conf",
                key: "password",
            }
        }
    },
    {
        name: "MOTHERSHIP_ID",
        valueFrom: {
            configMapKeyRef: {
                name: "mothership-conf",
                key: "mothership_id",
            }
        }
    },
];

export const POSTGRESQL_USERNAME = "username";
// needs to be at least 8 characters
export const POSTGRESQL_PASSWORD = "password";

export function ReadinessProbe(metricPort: number) {
    return {
        initialDelaySeconds: 2,
        periodSeconds: 2,
        successThreshold: 1,
        timeoutSeconds: 1,
        httpGet: {
            path: "/ready",
            port: metricPort
        }
    }
}