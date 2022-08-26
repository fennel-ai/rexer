import { LocalWorkspace } from "@pulumi/pulumi/automation";
const port = process.env.PORT || 3000;
const workspace = await LocalWorkspace.create({
    workDir: "../launchpad"
});
function filterTier(summary) {
    if (summary.name.startsWith("tier")) {
        var ret = false;
        workspace.getConfig("fennel/launchpad/" + summary.name, "pricingMode").then(v => {
            if (v.value === "FREE") {
                ret = true;
            }
        }).catch(_ => { console.log("failed to get pricing mode for: " + summary.name); });
        return ret;
    }
    return false;
}
// List Demo tiers.
const listDemoTiersHandler = (req, res) => {
    res.statusCode = 200;
    res.setHeader("Content-Type", "application/json");
    var result = [];
    const summary = workspace.listStacks().then(v => {
        for (var val of v) {
            if (filterTier(val)) {
                result.push(JSON.stringify(val));
            }
        }
        res.end(JSON.stringify(result));
    });
};
const getTierConfigHandler = (req, res) => {
    res.end("OK");
};
const notFoundHandler = (req, res) => {
    res.statusCode = 404;
    res.end("path not found");
};
export const router = (req, res) => {
    var handler;
    const path = req.url;
    console.log("PATH = " + path + ", Method = " + req.method);
    if (req.method != "GET") {
        handler = notFoundHandler;
    }
    else {
        switch (path) {
            case "/list_demo_tiers":
                handler = listDemoTiersHandler;
                break;
            case "/get_tier_config":
                handler = getTierConfigHandler;
                break;
            default:
                handler = notFoundHandler;
        }
    }
    handler(req, res);
};
//# sourceMappingURL=router.js.map