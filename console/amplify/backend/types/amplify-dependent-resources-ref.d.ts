export type AmplifyDependentResourcesAttributes = {
    "function": {
        "consoleBff": {
            "Name": "string",
            "Arn": "string",
            "Region": "string",
            "LambdaExecutionRole": "string"
        }
    },
    "auth": {
        "console": {
            "IdentityPoolId": "string",
            "IdentityPoolName": "string",
            "UserPoolId": "string",
            "UserPoolArn": "string",
            "UserPoolName": "string",
            "AppClientIDWeb": "string",
            "AppClientID": "string"
        }
    },
    "api": {
        "bff": {
            "RootUrl": "string",
            "ApiName": "string",
            "ApiId": "string"
        }
    }
}