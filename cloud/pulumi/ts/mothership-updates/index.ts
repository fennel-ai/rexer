import createConnectionPool, { ConnectionPool, sql } from '@databases/mysql';
import { LocalWorkspace } from "@pulumi/pulumi/automation/localWorkspace";
import { fullyQualifiedStackName, OutputMap, OutputValue } from "@pulumi/pulumi/automation";
import { Scope } from "../lib/util";
import * as jq from "node-jq";

// Create a mothership database connection.
const workspace = await LocalWorkspace.create({
    workDir: "../launchpad"
})

export type Customer = {
    id: number,
    name: string,
    domain: string,
}
const org = "fennel"
const project = "launchpad"

function getFullyQualifiedStackName(name: string): string {
    return fullyQualifiedStackName(org, project, name)
}

function getStackName(scope: Scope, scopeId: number): string {
    switch (scope) {
        case Scope.DATAPLANE:
            return `plane-${scopeId}`
        case Scope.MOTHERSHIP:
            return `mothership-${scopeId}`
        case Scope.TIER:
            return `tier-${scopeId}`
        default:
            return 'invalid'
    }
}

export async function getProperty(data: any, path: string[]): Promise<string[]> {
    var dataJsonStr = JSON.stringify(data)
    dataJsonStr = dataJsonStr.replaceAll("launchpad:", "")
    var result: string[] = []
    var getPath = async (index: number): Promise<string[]> => {
        if (index >= path.length) {
            return result
        }
        return jq.run(path[index], dataJsonStr, { input: "string" }).then(value => {
            result.push(String(value).split('"').join(''))
            return getPath(index + 1)
        })
    }
    return getPath(0).then(v => {
        return v;
    })
}

export class MothershipDBUpdater {
    id: number
    db: Promise<ConnectionPool>
    constructor(id: number) {
        this.id = id
        const stackName = getFullyQualifiedStackName(getStackName(Scope.MOTHERSHIP, this.id))
        this.db = workspace.stackOutputs(stackName).then((output: OutputMap) => {
            return getProperty(output, [".db.value.host", ".db.value.user", ".db.value.password", ".db.value.dbName"]).then(values => {

                const connStr = `mysql://${values[1]}:${values[2]}@${values[0]}/${values[3]}`
                console.log(`mothership db connection string : ${connStr}`)
                return createConnectionPool({
                    connectionString: connStr,
                    poolSize: 1,
                    queueTimeoutMilliseconds: 120 * 1000,
                })
            })
        })
    }
    async exit(): Promise<void> {
        await (await this.db).dispose()
    }

    async insertOrUpdateTier(tierId: number): Promise<void> {
        const stackName = getFullyQualifiedStackName(getStackName(Scope.TIER, tierId))
        return workspace.stackOutputs(stackName).then(output => {
            const time = Date.now()
            return getProperty(output, [".planeId.value", ".ingress.value.loadBalancerUrl"]).then(values => {
                const apiUrl = `http://${values[1]}/data`
                const planeId = values[0]
                const planeStackName = getFullyQualifiedStackName(getStackName(Scope.DATAPLANE, +planeId))
                return workspace.stackOutputs(planeStackName).then(poutput => {
                    return getProperty(poutput, [".customerId.value"]).then(customerId => {
                        return workspace.refreshConfig(stackName).then(configMap => {
                            return getProperty(configMap, [".plan.value", ".requestLimit.value"]).then(planAndLimits => {
                                return this.db.then(db => {
                                    db.query(sql`INSERT INTO tier (tier_id, data_plane_id, customer_id, pulumi_stack, api_url, k8s_namespace, deleted_at, created_at, updated_at, requests_limit, plan)
                                VALUES (${tierId}, ${+planeId}, ${+customerId[0]}, ${stackName}, ${apiUrl}, 't-${tierId}', 0, ${time}, ${time}, ${+planAndLimits[1]}, ${+planAndLimits[0]})
                                ON DUPLICATE KEY UPDATE data_plane_id=${+planeId}, customer_id=${+customerId[0]}, pulumi_stack=${stackName}, api_url=${apiUrl}, k8s_namespace='t-${tierId}', updated_at=${time}, requests_limit=${+planAndLimits[1]}, plan=${+planAndLimits[0]}`)
                                })
                            })

                        })

                    })
                })
            })
        })
    }
    async insertOrUpdateDataPlane(planeId: number, getCustomer: (id: number) => Customer | undefined): Promise<void> {
        const time = Date.now()
        await this.insertOrUpdateCustomer(planeId, getCustomer)
        const planeStackName = getFullyQualifiedStackName(getStackName(Scope.DATAPLANE, +planeId))
        return workspace.stackOutputs(planeStackName).then(output => {
            console.log(output)
            return getProperty(output, [".region.value", ".vpc.value.vpcId", ".roleArn.value", ".prometheus.value.loadBalancerURL"]).then(dataArr => {
                return this.db.then(db => {
                    console.log(dataArr)
                    const region = dataArr[0]
                    const vpcId = dataArr[1]
                    const awsRole = dataArr[2]
                    const metricsServerUrl = `http://${dataArr[3]}`
                    console.log("metrics server url " + metricsServerUrl)

                    db.query(sql`INSERT INTO data_plane (data_plane_id, aws_role, region, pulumi_stack, vpc_id, deleted_at, created_at, updated_at, metrics_server_address)
                    VALUES (${planeId}, ${awsRole}, ${region}, ${planeStackName}, ${vpcId}, 0, ${time}, ${time}, ${metricsServerUrl})
                    ON DUPLICATE KEY UPDATE data_plane_id=${planeId}, aws_role=${awsRole}, region=${region}, pulumi_stack=${planeStackName}, vpc_id=${vpcId}, updated_at=${time}, metrics_server_address=${metricsServerUrl}`)
                })
            })
        })
    }

    async insertOrUpdateCustomer(planeId: number, getCustomer: (id: number) => Customer | undefined): Promise<void> {
        const planeStackName = getFullyQualifiedStackName(getStackName(Scope.DATAPLANE, +planeId))
        return workspace.stackOutputs(planeStackName).then(poutput => {
            return getProperty(poutput, [".customerId.value"]).then(customerId => {
                const customer = getCustomer(+customerId[0])
                const time = Date.now()
                if (customer !== undefined) {
                    return this.db.then(db => {
                        db.query(sql`INSERT INTO customer (customer_id, name, domain, deleted_at, created_at, updated_at)
                                VALUES (${+customerId[0]}, ${customer.name}, ${customer.domain}, 0, ${time}, ${time})
                                ON DUPLICATE KEY UPDATE name=${customer.name}, domain=${customer.domain}, updated_at=${time}`)
                    })
                }
                return
            })
        })
    }
}

