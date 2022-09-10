import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

export const plugins = {
    "aws": "v5.0.0",
}

export type inputType = {
    region: string,
    roleArn: pulumi.Input<string>,
    nodeInstanceRole: pulumi.Output<string>,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

const AMAZON_SAGE_MAKER_FULL_ACCESS_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess";

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("plane-eks-instance-iam-policy-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const nodeSagemakerPolicyAttach = new aws.iam.RolePolicyAttachment(`node-sagemaker-policy-attach`, {
        policyArn: AMAZON_SAGE_MAKER_FULL_ACCESS_POLICY_ARN,
        role: input.nodeInstanceRole,
    }, { provider: provider });

    return pulumi.output({})
}
