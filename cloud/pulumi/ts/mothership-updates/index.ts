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

async function getProperty(outputMap: OutputMap, path: string): Promise<string> {
    return jq.run(path, JSON.stringify(outputMap), { input: "string" }).then(value => {
        return String(value).split('"').join('')
    })
}

export class MothershipDBUpdater {
    id: number
    db: Promise<ConnectionPool>
    constructor(id: number) {
        this.id = id
        const stackName = getFullyQualifiedStackName(getStackName(Scope.MOTHERSHIP, this.id))
        this.db = workspace.stackOutputs(stackName).then((output: OutputMap) => {
            return getProperty(output, ".db.value.host").then(host => {
                return getProperty(output, ".db.value.user").then(user => {
                    return getProperty(output, ".db.value.password").then(password => {
                        return getProperty(output, ".db.value.dbName").then(dbName => {
                            const connStr = `mysql://${user}:${password}@${host}/${dbName}`
                            console.log(`mothership db connection string : ${connStr}`)
                            return createConnectionPool(connStr)
                        })
                    })
                })
            })
        });
    }
    async exit(): Promise<void> {
        await (await this.db).dispose()
    }

    async insertOrUpdateTier(tierId: number): Promise<void> {
        const stackName = getFullyQualifiedStackName(getStackName(Scope.TIER, tierId))
        return workspace.stackOutputs(stackName).then(output => {
            const time = Date.now()
            return getProperty(output, ".planeId.value").then(planeId => {
                return getProperty(output, ".ingress.value.loadBalancerUrl").then(apiUrl => {
                    const planeStackName = getFullyQualifiedStackName(getStackName(Scope.DATAPLANE, +planeId))
                    return workspace.stackOutputs(planeStackName).then(poutput => {
                        return getProperty(poutput, ".customerId.value").then(customerId => {
                            return this.db.then(db => {
                                db.query(sql`INSERT INTO tier (tier_id, data_plane_id, customer_id, pulumi_stack, api_url, k8s_namespace, deleted_at, created_at, updated_at)
                                VALUES (${tierId}, ${+planeId}, ${+customerId}, ${stackName}, ${apiUrl}, 't-${tierId}', 0, ${time}, ${time})
                                ON DUPLICATE KEY UPDATE data_plane_id=${+planeId}, customer_id=${+customerId}, pulumi_stack=${stackName}, api_url=${apiUrl}, k8s_namespace='t-${tierId}', updated_at=${time}`)
                            })
                        })
                    })
                })
            })
        })
    }
    async insertOrUpdateCustomer(planeId: number, getCustomer: (id: number) => Customer | undefined): Promise<void> {
        const planeStackName = getFullyQualifiedStackName(getStackName(Scope.DATAPLANE, +planeId))
        return workspace.stackOutputs(planeStackName).then(poutput => {
            return getProperty(poutput, ".customerId.value").then(customerId => {
                const customer = getCustomer(+customerId)
                const time = Date.now()
                if (customer !== undefined) {
                    return this.db.then(db => {
                        db.query(sql`INSERT INTO customer (customer_id, name, domain, deleted_at, created_at, updated_at)
                                VALUES (${customerId}, ${customer.name}, ${customer.domain}, 0, ${time}, ${time})
                                ON DUPLICATE KEY UPDATE name=${customer.name}, domain=${customer.domain}, updated_at=${time}`)
                    })
                }
                return
            })
        })
    }
}

