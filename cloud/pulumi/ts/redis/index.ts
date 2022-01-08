import { memorydb, ec2 } from "@pulumi/aws-native";

const REDIS_VERSION = "6.2";
const NODE_TYPE = "db.t4g.small";
// TODO: Remove hard-coded subnet ids and use subnets created in non-default VPC.
const subnetIds = ["subnet-0ac6f13fe8fcd49ef"];
// TODO: Increase replica count once we add more than one subnet to group.
const NUM_REPLICAS = 0;

const subnetGroup = new memorydb.SubnetGroup("redis-subnet-group",
    {
        subnetIds: subnetIds,
    }
)

const cluster = new memorydb.Cluster("redis-db",
    {
        subnetGroupName: subnetGroup.id,
        aCLName: "open-access",
        engineVersion: REDIS_VERSION,
        nodeType: NODE_TYPE,
        autoMinorVersionUpgrade: true,
        tLSEnabled: true,
        numReplicasPerShard: NUM_REPLICAS,
    }
)

// Export the name of the cluster
export const clusterName = cluster.id;
