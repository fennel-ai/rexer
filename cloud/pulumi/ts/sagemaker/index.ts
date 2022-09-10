import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";

import { fennelStdTags } from "../lib/util";

export const plugins = {
    "aws": "v5.0.0"
}

export type inputType = {
    region: string,
    roleArn: string,
    planeId: number,
    tierId: number,
    vpcId: string,
    connectedSecurityGroups: Record<string, string>,
    modelStoreBucket: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    subnetIds: string[],
    securityGroup: string,
    roleArn: string
}

const AMAZON_SAGE_MAKER_FULL_ACCESS_POLICY_ARN = "arn:aws:iam::aws:policy/AmazonSageMakerFullAccess"

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const provider = new aws.Provider("sagemaker-aws-provider", {
        region: <aws.Region>input.region,
        assumeRole: {
            roleArn: input.roleArn,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const subnetIds = await aws.ec2.getSubnetIds({
            vpcId: input.vpcId,
            // TODO: use better method for filtering private subnets.
            filters: [{
                name: "tag:Name",
                values: [`p-${input.planeId}-primary-private-subnet`, `p-${input.planeId}-secondary-private-subnet`],
            }]
        }, { provider });

    const sagemakerSg = new aws.ec2.SecurityGroup(`t-${input.tierId}-sagemaker-sg`, {
        vpcId: input.vpcId,
        tags: { ...fennelStdTags }
    }, { provider });

    // Allow traffic from EKS worker nodes
    let sgRules: pulumi.Output<string>[] = []
    for (var key in input.connectedSecurityGroups) {
        sgRules.push(new aws.ec2.SecurityGroupRule(`t-${input.tierId}-sagemaker-allow-${key}`, {
            securityGroupId: sagemakerSg.id,
            sourceSecurityGroupId: input.connectedSecurityGroups[key],
            fromPort: 0,
            toPort: 65535,
            type: "ingress",
            protocol: "tcp",
        }, { provider }).id)
    }

    // create IAM role and give access to read S3 buckets where models are stored
    const role = new aws.iam.Role(`t-${input.tierId}-sagemaker-role`, {
        namePrefix: `t-${input.tierId}-sagemakerrole-`,
        description: `IAM role for AWS sagemaker for tier ${input.tierId}`,
        assumeRolePolicy: `{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Service": "sagemaker.amazonaws.com"
                    },
                    "Action": "sts:AssumeRole"
                }
            ]
        }`,
    }, { provider });

    // create inline role policy
    const policy = new aws.iam.RolePolicy(`t-${input.tierId}-sagemaker-rolepolicy`, {
        name: `t-${input.tierId}-sagemaker-rolepolicy`,
        role: role,
        policy: `{
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect":"Allow",
                    "Action": "s3:ListBucket",
                    "Resource": "arn:aws:s3:::${input.modelStoreBucket}"
                },
                {
                    "Effect":"Allow",
                    "Action": [
                        "s3:PutObject",
                        "s3:GetObject",
                        "s3:DeleteObject"
                    ],
                    "Resource": "arn:aws:s3:::${input.modelStoreBucket}/*"
                }
            ]
        }`,
    }, { provider });

    // attach sagemaker full access to the sagemaker execution role
    const attachSagemakerRolePolicy = new aws.iam.RolePolicyAttachment(`t-${input.tierId}-sagemaker-fullaccess-execrole`, {
        policyArn: AMAZON_SAGE_MAKER_FULL_ACCESS_POLICY_ARN,
        role: role,
    }, { provider: provider });

    return pulumi.output({
        subnetIds: subnetIds.ids,
        securityGroup: sagemakerSg.name,
        roleArn: role.arn,
    });
}
