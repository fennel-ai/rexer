import { ConfigMap, LocalWorkspace, OutputMap, StackSummary } from "@pulumi/pulumi/automation";
import { IncomingMessage, ServerResponse } from "http";
import * as url from "url";

const port = process.env.PORT || 3000
const workspace = await LocalWorkspace.create({
    workDir: "../launchpad"
})

const stackPrefix = "fennel/launchpad"

function getFullyQualifiedStackName(name: string): string {
    return `${stackPrefix}/${name}`
}


// List Demo tiers.
const listStacksHandler = (req: IncomingMessage, res: ServerResponse) => {
    res.statusCode = 200
    res.setHeader("Content-Type", "application/json")
    var result: string[] = []
    const summary = workspace.listStacks().then(v => {
        for (var val of v) {
            result.push(JSON.stringify(val))
        }
        res.end(JSON.stringify(result))
    })

}

class TierDetails {
    Config?: ConfigMap
    Output?: OutputMap
    constructor(config?: ConfigMap, output?: OutputMap) {
        this.Config = config
        this.Output = output
    }
}

const getStackDetailsHandler = (req: IncomingMessage, res: ServerResponse) => {
    res.statusCode = 200
    const emptyResult = JSON.stringify({})
    if (req.url !== undefined) {
        const queryString = url.parse(req.url, true, true)
        try {
            const stackName = queryString.query["stack_name"]
            workspace.getAllConfig(getFullyQualifiedStackName(`${stackName}`)).then(config => {
                workspace.stackOutputs(getFullyQualifiedStackName(`${stackName}`)).then(output => {
                    res.end(JSON.stringify(<TierDetails>({ config: config, output: output })))
                }).catch(e => {
                    console.log(`failed to get output for stack: ${stackName}, err: ${e}`)
                    res.end(emptyResult)
                })

            }).catch(e => {
                console.log(`failed to get config for stack: ${stackName}, err: ${e}`)
                res.end(emptyResult)
            })
            return
        } catch (e) {
            console.log("error while getting config: " + e)
            res.end(emptyResult)
        }
        return
    }
    res.end(emptyResult)
}

const notFoundHandler = (req: IncomingMessage, res: ServerResponse) => {
    res.statusCode = 404
    res.end("path not found")
}

export const router = (req: IncomingMessage, res: ServerResponse) => {
    var handler: (req: IncomingMessage, res: ServerResponse) => void
    const searchIndex = req.url?.indexOf("?")
    var path: string | undefined
    if (searchIndex !== undefined && searchIndex > 0) {
        path = req.url?.substring(0, searchIndex)!
    } else {
        path = req.url
    }
    console.log("PATH = " + path + ", Method = " + req.method)
    if (req.method != "GET") {
        handler = notFoundHandler
    } else {
        switch (path) {
            case "/list_stacks":
                handler = listStacksHandler
                break
            case "/get_stack_details":
                handler = getStackDetailsHandler
                break
            default:
                handler = notFoundHandler
        }
    }
    handler(req, res)
}

