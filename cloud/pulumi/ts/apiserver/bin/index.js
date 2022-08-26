import * as http from "http";
import * as process from "process";
import { router } from "./router";
const port = process.env.PORT || 3000;
// Currently we expose only read-only APIs to the following.
// 1. List stacks with a filter.
// 2. Select and summarize a stack.
const server = http.createServer((req, res) => {
    router(req, res);
});
server.listen(port, () => {
    console.log(`Server running at port ${port}`);
});
//# sourceMappingURL=index.js.map