// operator for type-safety for string key access:
// https://schneidenbach.gitbooks.io/typescript-cookbook/content/nameof-operator.html
export const nameof = <T>(name: keyof T) => name;

// Tags to be added to all fennel-managed aws resources.
export const fennelStdTags = {
    "managed-by": "fennel.ai",
}

// Kubernetes resource spec
export type ResourceSpec = {
    limit: string,
    request: string,
}

// Configuration for a kubernetes resource
export type ResourceConf = {
    // CPU resource spec for the kubernetes resource
    //
    // This must be of the form:
    //  https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-cpu
    cpu: ResourceSpec,
    // Memory resource spec for the kubernetes resource
    //
    // This must be of the form:
    //  https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#meaning-of-memory
    memory: ResourceSpec,
}