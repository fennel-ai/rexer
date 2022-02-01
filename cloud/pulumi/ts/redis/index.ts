import * as pulumi from "@pulumi/pulumi"
import * as aws from "@pulumi/aws";

const REDIS_VERSION = "6.2";
const NODE_TYPE = "db.t4g.small";

// Get subnet id from stack configuration.
const config = new pulumi.Config();
const subnetIds = config.requireObject<string[]>("subnetIds")

// TODO: Increase replica count once we add more than one subnet to group.
const NUM_REPLICAS = 0;

const subnetGroup = new aws.memorydb.SubnetGroup("redis-subnet-group",
    {
        subnetIds: subnetIds,
    }
)

// TODO: Create security group to control access to redis instance and only allow
// traffic from EKS security group.
const cluster = new aws.memorydb.Cluster("redis-db",
    {
        subnetGroupName: subnetGroup.id,
        aclName: "open-access",
        engineVersion: REDIS_VERSION,
        nodeType: NODE_TYPE,
        autoMinorVersionUpgrade: true,
        tlsEnabled: true,
        numReplicasPerShard: NUM_REPLICAS,
    }
)

// Export the name of the cluster
export const clusterName = cluster.id;

export const clusterUrl = cluster.clusterEndpoints
