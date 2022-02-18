import * as pulumi from "@pulumi/pulumi"
import * as aws from "@pulumi/aws";

const REDIS_VERSION = "6.2";
const NODE_TYPE = "db.t4g.small";
// TODO: Increase replica count once we add more than one subnet to group.
const NUM_REPLICAS = 0;

// Get subnet id from stack configuration.
const config = new pulumi.Config();

async function setupRedisCluster(): Promise<aws.memorydb.Cluster> {

    const vpcId = config.require("vpcId")

    const subnets = await aws.ec2.getSubnetIds({
        vpcId
    })

    const subnetGroup = new aws.memorydb.SubnetGroup("redis-subnet-group",
        {
            // TODO: Setup only in private subnet ids.
            subnetIds: subnets.ids,
        }
    )

    const redisSg = new aws.ec2.SecurityGroup("redis-sg", {
        namePrefix: "redis-sg-",
        vpcId,
    })

    const allowEksTraffic = new aws.ec2.SecurityGroupRule("allow-eks", {
        securityGroupId: redisSg.id,
        sourceSecurityGroupId: config.require("eksSecurityGroup"),
        fromPort: 0,
        toPort: 65535,
        type: "ingress",
        protocol: "tcp",
    })

    const cluster = new aws.memorydb.Cluster("redis-db",
        {
            subnetGroupName: subnetGroup.id,
            aclName: "open-access",
            engineVersion: REDIS_VERSION,
            nodeType: NODE_TYPE,
            autoMinorVersionUpgrade: true,
            tlsEnabled: true,
            numReplicasPerShard: NUM_REPLICAS,
            securityGroupIds: [redisSg.id],
        }
    )


    return cluster
}


export = async () => {

    const cluster = await setupRedisCluster()


    const clusterId = cluster.id;
    const clusterEndPoints = cluster.clusterEndpoints
    const clusterSecurityGroupIds = cluster.securityGroupIds

    return { clusterId, clusterEndPoints, clusterSecurityGroupIds }
}