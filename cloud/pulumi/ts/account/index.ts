import * as pulumi from "@pulumi/pulumi"
import * as aws from "@pulumi/aws";

const ROOT_ID = "r-7try";


export const plugins = {
    "aws": "v5.0.0",
}

export const MASTER_ACCOUNT_ADMIN_ROLE_ARN = "arn:aws:iam::030813887342:role/admin";

export type inputType = {
    name: string,
    email: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {
    roleArn: string,
}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const roleName = "admin";
    // keep the pulumi resource name same as the account name
    const account = await new aws.organizations.Account(input.name, {
        // email id for the account being setup
        // ideally this should be `admin+{CUSTOMER_NAME}@fennel.ai`
        email: input.email,
        // only the root of this account should have access to this account billing information
        iamUserAccessToBilling: "DENY",
        // Name of this account
        name: input.name,
        // should be either Root ID for the account or Parent OU ID
        // currently these accounts are being created under the same parent organization unit (default), we set this
        // to root ID
        parentId: ROOT_ID,
        // The name of the IAM role which has admin access and adds the "management" or "master" account as a
        // trusted policy.
        //
        // This allows assuming admin access for the IAM users in the master account if configured.
        roleName: roleName,
    });

    const provider = new aws.Provider("account-aws-provider", {
        assumeRole: {
            // This should be the master account ARN
            roleArn: MASTER_ACCOUNT_ADMIN_ROLE_ARN,
            // TODO: Also populate the externalId field to prevent "confused deputy"
            // attacks: https://docs.aws.amazon.com/IAM/latest/UserGuide/confused-deputy.html
        }
    });

    const policyName = `assume-${input.name}-admin`
    const policyAttachName = `assume-${input.name}-admin-policyAttach`
    const accountAdminRoleArn = account.id.apply(accountId => { return `arn:aws:iam::${accountId}:role/${roleName}` });

    // create IAM policy in the master account which allows assuming admin access in this newly created account
    const policy = pulumi.all([account.id, accountAdminRoleArn]).apply(([accountId, roleArn]) => {
        return new aws.iam.Policy(policyName, {
            path: "/",
            namePrefix: policyName,
            description: `Policy to assume admin role in account ${accountId}.`,
            policy: JSON.stringify({
                Version: "2012-10-17",
                Statement: [{
                    Effect: "Allow",
                    Action: [
                        "sts:AssumeRole",
                    ],
                    // this role is automatically created for us when we setup the account (we give `admin` as the
                    // roleName.
                    Resource: roleArn,
                }]
            })
        }, { provider: provider });
    });
    // get admins usergroup
    const group = pulumi.output(aws.iam.getGroup({ groupName: "admins" }, { provider: provider }));

    // attach this IAM policy to the `admins` user group of the master account so that any user who joins/added
    // to the user group can assume admin access for the resources in this account
    const policyAttach = new aws.iam.PolicyAttachment(policyAttachName, {
        name: policyAttachName,
        policyArn: policy.arn,
        groups: [group.groupName],
    }, { provider: provider });

    return pulumi.output({
        roleArn: accountAdminRoleArn,
    });
}
