import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

const console = new aws.amplify.App("console", {
    basicAuthCredentials: "YWRtaW46Zm91bmRhdGlvbg==",
    enableBasicAuth: true,
    environmentVariables: {
        AMPLIFY_DIFF_DEPLOY: "false",
        AMPLIFY_MONOREPO_APP_ROOT: "console",
        _LIVE_UPDATES: "[{\"name\":\"Amplify CLI\",\"pkg\":\"@aws-amplify/cli\",\"type\":\"npm\",\"version\":\"latest\"}]",
    },
    iamServiceRoleArn: "arn:aws:iam::030813887342:role/amplifyconsole-backend-role",
    name: "console",
    platform: "WEB",
    repository: "https://github.com/fennel-ai/starql",
}, {
    protect: true,
});
