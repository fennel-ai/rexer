import * as pulumi from "@pulumi/pulumi";

export const plugins = {}

export type inputType = {}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    const output = pulumi.output({})
    return output
}
