import process from "process";
import childProcess from "child_process";
import readline from "readline";

import * as pulumi from "@pulumi/pulumi";
import * as docker from "@pulumi/docker";
import * as uuid from "uuid";

// Downside here is that we create a new image even when if there were no changes in the underlying code,
// restarting the pods relying on the image name returned.
export function DockerBuildMultiArchBuildx(dockerFile: string, imageName: string): pulumi.Output<string> {
    // In case docker login is required
    //
    // const dockerLogin = new local.Command("docker-login", {
    //     create: `docker login -u ${registry.username} -p ${registry.password}`,
    //     delete: `docker logout ${registry.server}`
    // });

    const root = process.env["FENNEL_ROOT"]!;
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageNameWithTag = `${imageName}:${hashId}-${uuid.v4()}`;

    const cmdOutput = await RunCommand('docker:buildx:buildPush', imageNameWithTag, 'docker',[
        'buildx', 'build', '--platform', 'linux/amd64,linux/arm64', root, '-f', dockerFile, '-t', imageNameWithTag, '--push'
    ]);
    return imageNameWithTag;
}


// TODO(mohit): Deprecate this once docker supports loading images built with `buildx`.
// See - https://github.com/docker/roadmap/issues/371 and https://github.com/docker/buildx/issues/59#issuecomment-488902807
//
// Once this is supported, we should be able to use `DockerBuildMultiArchIdempotent`.
//
// Down side of this mechanism is that
export function DockerBuildMultiArch(binaryName: string, amdDockerfile: string, armDockerFile: string, imageName: string, registryInfo: any): pulumi.Output<string> {
    // In case docker login is required
    //
    // const dockerLogin = new local.Command("docker-login", {
    //     create: `docker login -u ${registry.username} -p ${registry.password}`,
    //     delete: `docker logout ${registry.server}`
    // });

    const root = process.env["FENNEL_ROOT"]!;
    const hashId = childProcess.execSync('git rev-parse --short HEAD').toString().trim()
    const imageNameWithTag = `${imageName}:${hashId}`;
    // build amd64 image
    const amdImage = new docker.Image(`${binaryName}-amd64`, {
        build: {
            context: root,
            dockerfile: amdDockerfile,
        },
        imageName: `${imageNameWithTag}-amd64`,
        registry: registryInfo,
    });

    // build arm64 image
    const armImage = new docker.Image(`${binaryName}-arm64`, {
        build: {
            context: root,
            dockerfile: armDockerFile,
        },
        imageName: `${imageNameWithTag}-arm64`,
        registry: registryInfo,
    });

    // create manifest referring to both the images
    return pulumi.all([armImage.imageName, amdImage.imageName]).apply
    (async ([armImageName, amdImageName]) => {
        let cmdOutput: CommandResult;
        // TODO(mohit): imageNameWithTag here should be a hash from the SHA of `amdImage` and `armImage` so that
        // if the underlying images have changed, we should create a new image index/manifest or use an existing one
        //
        // Downside is that the hash could potentially lead to a collision, resolving in an existing, potentially
        // old image being used
        cmdOutput = await RunCommand("docker:manifest:create", imageNameWithTag, 'docker', [
            'manifest', 'create', imageNameWithTag, '--amend', armImageName, '--amend', amdImageName,
        ]);
        if (cmdOutput.code != 0) {
            console.error('failed to create docker manifest. Exiting')
            process.exit(1);
        }

        // push the manifest
        cmdOutput = await RunCommand("docker:manifest:push", imageNameWithTag, 'docker', [
            'manifest', 'push', imageNameWithTag
        ]);
        if (cmdOutput.code != 0) {
            console.error('failed to push docker manifest. Exiting');
            process.exit(1);
        }
        return imageNameWithTag;
    });
}

// TODO(mohit): Deprecate this once pulumi supports building multi-arch images.
//
// For that, either pulumi will need to support buildx - https://github.com/pulumi/pulumi-docker/issues/296
//
// Or provide a way to create a docker manifest and push.
//
// MultiArch images can be built using either of the mechanisms - https://www.docker.com/blog/multi-arch-build-and-images-the-simple-way/
export async function DockerBuildMultiArchIdempotent(logName: string, baseImageName: string, dockerfile: string, root: string, tag: string): Promise<string> {
    // In case docker login is required
    //
    // const dockerLogin = new local.Command("docker-login", {
    //     create: `docker login -u ${registry.username} -p ${registry.password}`,
    //     delete: `docker logout ${registry.server}`
    // });

    const imageNameWithTag = `${baseImageName}:${tag}`;

    // here using pulumi's local.Command is not useful - it is possible that it's input have not changed but we
    // want to command to execute
    let cmdOutput: CommandResult;
    cmdOutput = await RunCommand('docker:buildx:build', logName, 'docker', ['buildx', 'build', '--platform', 'linux/amd64,linux/arm64', root, '-f', dockerfile, '-t', imageNameWithTag]);
    if (cmdOutput.code != 0) {
        console.error('docker:buildx:build ', logName, ' docker build failed. Exiting');
        process.exit(1);
    }

    // now check the sha of the image built; this prevents creating a new image in the registry even if it was
    // not changed (and the deployment spec does not change).
    cmdOutput = await RunCommand('docker:image:inspect', logName, 'docker', ['image', 'inspect', '-f', '{{.Id}}', imageNameWithTag]);
    if (cmdOutput.code != 0) {
        console.error('docker:image:inspect ', logName, ' docker inspect failed. Exiting');
        process.exit(1);
    }
    // use the sha to tag the image
    if (!cmdOutput.stdout) {
        console.error(logName, ' expected image sha to be non-empty');
        process.exit(1);
    }
    // the sha is of the format `algo:hash`;
    const imageSha = cmdOutput.stdout.trim();
    const idx = imageSha.lastIndexOf(':');
    const imageId = idx < 0 ? imageSha : imageSha.substring(idx + 1);
    const imageNameWithTagImageId = `${baseImageName}:${tag}-${imageId}`;

    // tag the image and push - if it already exists, this is a no-op. Else a new image is created.
    //
    // we create image with two tags - 1. `tag` which is the commit sha 2. `tag-imgSha` commit sha and image sha combined
    // 1. helps with quickly identifying the commit message at which the image was built
    // 2. helps with differentiating two different images built with different local changes.
    await tagAndPush(imageNameWithTag);
    await tagAndPush(imageNameWithTagImageId);

    async function tagAndPush(targetName: string) {
        let cmdOutput = await RunCommand('docker:tag', logName, 'docker', ['tag', imageNameWithTag, targetName]);
        if (cmdOutput.code != 0) {
            console.error('docker:tag ', logName, ' docker tag failed for image: ', imageNameWithTag, ' target: ', targetName);
            process.exit(1);
        }
        cmdOutput = await RunCommand('docker:push', logName, 'docker', ['push', targetName]);
        if (cmdOutput.code != 0) {
            console.error('docker:push ', logName, ' docker push failed for image: ', targetName);
            process.exit(1);
        }
        return;
    }

    return imageNameWithTagImageId;
}

export type CommandResult = {
    code: number,
    stdout: string,
}

// Runs the command with the args and environment vars and returns the code and the standard output
//
// This is copied from - https://github.com/pulumi/pulumi-docker/blob/038b9e9c1441d5412b4df1c5f49e65ddc3003f33/sdk/nodejs/docker.ts#L613
// which is used by the docker pulumi plugin and refactored a bit to avoid any pulumi dependencies
async function RunCommand(commandName: string, logName: string, command: string, args: string[], env?: {[name: string]: string}): Promise<CommandResult> {
    console.log(commandName, ' ', logName, ' Running: ', commandName, args);
    return new Promise<CommandResult>((resolve, reject) => {
        const osEnv = Object.assign({}, process.env);
        env = Object.assign(osEnv, env)
        const p = childProcess.spawn(command, args, { env });
        // We store the results from stdout in memory and will return them as a string.
        let stdOutChunks: Buffer[] = [];
        let stdErrChunks: Buffer[] = [];
        p.stdout.on("data", (chunk: Buffer) => stdOutChunks.push(chunk));
        p.stderr.on("data", (chunk: Buffer) => stdErrChunks.push(chunk));

        const rl = readline.createInterface({ input: p.stdout });
        rl.on("line", line => console.log(commandName, ' ', logName, ' ', line));

        p.on("error", err => {
            // received some sort of real error.  push the message of that error to our stdErr
            // stream (so it will get reported) and then move this promise to the resolved, 1-code
            // state to indicate failure.
            stdErrChunks.push(new Buffer(err.message));
            finish(/*code: */ 1);
        });

        p.on("close", code => {
            if (code === null) {
                finish(/*code: */ 0);
            } else {
                finish(code);
            }
        });

        return;

        function finish(code: number) {
            // Collapse our stored stdout/stderr messages into single strings.
            const stderr = Buffer.concat(stdErrChunks).toString();
            const stdout = Buffer.concat(stdOutChunks).toString();

            // Clear out our output buffers.  This ensures that if we get called again, we don't
            // double print these messages.
            stdOutChunks = [];
            stdErrChunks = [];

            // If we got any stderr messages, report them as an error/warning depending on the
            // result of the operation.
            if (stderr.length > 0) {
                if (code) {
                    // Command returned non-zero code.  Treat these stderr messages as an error.
                    console.error(commandName, ' ', logName, ' ', stderr);
                } else {
                    // command succeeded.  These were just warning.
                    console.warn(commandName, ' ', logName, ' ', stderr);
                }
            }

            // If the command failed report an ephemeral message indicating which command it was.
            // That way the user can immediately see something went wrong in the info bar.  The
            // caller (normally runCommandThatMustSucceed) can choose to also report this
            // non-ephemerally.
            if (code) {
                console.error(commandName, ' ', logName, ' failed to run the command: ', command, args, 'status code: ', code);
            }

            resolve({ code, stdout });
        }
    });
}
