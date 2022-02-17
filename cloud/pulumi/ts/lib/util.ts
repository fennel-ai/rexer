// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// Tags to be added to all fennel-managed aws resources.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}
