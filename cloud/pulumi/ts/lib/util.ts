import * as pulumi from "@pulumi/pulumi";
import { local } from "@pulumi/command";
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

// BuildMultiArchImage builds multi-arch/multi-platform images using `buildx`
//
// buildx currently has a limitation (https://github.com/docker/buildx/issues/59) which does not allow us to build an
// image and then inspect/look it up - this is useful in avoiding pushing a new image when the image
// has not changed (i.e. it's checksum is unchanged).
//
// Previously we have tagged images with the commit SHA of HEAD. However, it is possible that there were uncommitted
// local changes. Using pulumi's docker provider, this was possible to get around as it would inspect the image built
// and tag the image additionally with its SHA value. This allowed de-duplicating images when it's content has not
// changed, but also allowed considering local changes.
//
// We get around `buildx` limitation by appending an uuid to the tag.
// NOTE: This will result in a new image on every update (hence triggering restarts in the pods using the image) even
// its content has not changed
//
// TODO(mohit): Replace this with https://github.com/fennel-ai/rexer/pull/1305 once buildx issue is resolved OR
// use pulumi buildx once it's implemented - https://github.com/pulumi/pulumi-docker/issues/296 OR
// switch to using materliaze docker provider which does exact same at a much lower maintenance burden -
// https://github.com/MaterializeInc/pulumi-docker-buildkit/issues/21
export function BuildMultiArchImage(binName: string, root: string, dockerfile: string, imageName: pulumi.Output<string>): pulumi.Output<local.Command> {
    // In case docker login is required
    //
    // const dockerLogin = new local.Command("docker-login", {
    //     create: `docker login -u ${registry.username} -p ${registry.password}`,
    //     delete: `docker logout ${registry.server}`
    // });
    const imgBuildPush = imageName.apply(imageName => {
        return new local.Command(binName, {
            create: `docker buildx build --platform linux/amd64,linux/arm64 ${root} -f ${dockerfile} -t ${imageName} --push`,
            // create a replacement for the command so that the command on creation is run everytime (if we don't
            // replace this, the pulumi resource corresponding to the command will be updated and returned immediately).
        }, { deleteBeforeReplace: true, replaceOnChanges: ['*'] });
    });

    imgBuildPush.stdout.apply(buildOut => {
        console.log(`${binName} build push stdout: `, buildOut);
    });

    imgBuildPush.stderr.apply(buildErr => {
        console.log(`${binName} build push stderr `, buildErr);
    });
    return imgBuildPush;
}