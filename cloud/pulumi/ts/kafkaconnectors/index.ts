import * as pulumi from "@pulumi/pulumi";
import * as confluent from "@pulumi/confluent";

export const plugins = {
    "confluent": "v0.2.2",
}

export type inputType = {
    tierId: number,
    // confluent configurations
    username: string,
    password: string,
    clusterId: string,
    environmentId: string,
    // kafka configurations
    kafkaApiKey: string,
    kafkaApiSecret: pulumi.Input<string>,
    // AWS and S3 configurations
    awsAccessKeyId: string,
    awsSecretAccessKey: string,
    s3BucketName: string,
}

// should not contain any pulumi.Output<> types.
export type outputType = {}

export const setup = async (input: inputType): Promise<pulumi.Output<outputType>> => {
    // create AWS buckets for each of the connectors
    const confProvider = new confluent.Provider(`t-${input.tierId}-conn-conf-provider`, {
        username: input.username,
        password: input.password,
    });

    const connName = `t-${input.tierId}-connector`;
    // https://docs.confluent.io/cloud/current/connectors/cc-s3-sink.html#configuration-properties
    const connector = new confluent.Connector(connName, {
        name: connName,
        clusterId: input.clusterId,
        environmentId: input.environmentId,
        // https://docs.confluent.io/cloud/current/connectors/cc-s3-sink.html#step-2-show-the-required-connector-configuration-properties
        config: {
            "name": connName,
            "connector.class": "S3_SINK",
            "kafka.auth.mode": "KAFKA_API_KEY",
            "kafka.api.key": input.kafkaApiKey,
            "kafka.api.secret": input.kafkaApiSecret,
            "aws.access.key.id": input.awsAccessKeyId,
            "aws.secret.access.key": input.awsSecretAccessKey,
            "input.data.format": "JSON",
            "output.data.format": "JSON",
            "s3.bucket.name": input.s3BucketName,
            "time.interval" : "HOURLY",
            "flush.size": "1000000",  // 1M
            // TODO: monitor lag and increment accordingly
            "tasks.max": "1",
            // the bucket "directory" has the following format:
            //  `s3://<s3-bucket-name>/${topics.dir}/<Topic-Name>/${path.format}/<files>`
            "topics.dir": `daily`,
            // `path.format` has the default value of: `‘year’=YYYY/’month’=MM/’day’=dd/’hour’=HH` - we will use this
            "topics": `t_${input.tierId}_actionlog_json,t_${input.tierId}_featurelog`,
        },
    }, {provider: confProvider})

    const output = pulumi.output({})
    return output
}
