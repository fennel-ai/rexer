import * as aws from "@pulumi/aws";

const serverRepo = new aws.ecr.Repository("starql-server", {
    imageScanningConfiguration: {
        scanOnPush: true,
    },
    name: "starql-server",
    imageTagMutability: "MUTABLE",
});

export const serverRepoArn = serverRepo.arn
