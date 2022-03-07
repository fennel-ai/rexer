package lib

const (
	MaxRegionLength                = 16
	MaxCustomerNameLength          = 32
	MaxTierK8sNamespaceLength      = 32
	MaxAWSRoleLength               = 32
	MaxVPCIDLength                 = 32
	MaxEKSInstanceTypeLength       = 32
	MaxConfluentEnvironmentLength  = 32
	MaxConfluentClusterIDLength    = 32
	MaxConfluentClusterNameLength  = 32
	MaxClusterSecutityGroupLength  = 32
	MaxDBUsernameLength            = 32
	MaxDBPasswordLength            = 32
	MaxClusterIDLength             = 64
	MaxKafkaBootstrapServersLength = 128
	MaxKafkaAPIKeyLength           = 128
	MaxKafkaSecretKeyLength        = 128
	MaxHostnameLength              = 128
	MaxPulimiStackLength           = 128
	MaxTierAPIURLLength            = 256
)

type Customer struct {
	CustomerID uint32 `db:"customer_id"`
	Name       string `db:"name"`
}

type Tier struct {
	TierID       uint32 `db:"tier_id"`
	DataPlaneID  uint32 `db:"data_plane_id"`
	CustomerID   uint32 `db:"customer_id"`
	PulumiStack  string `db:"pulumi_stack"`
	APIURL       string `db:"api_url"`
	K8sNamespace string `db:"k8s_namespace"`
}

type DataPlane struct {
	DataPlaneID           uint32 `db:"data_plane_id"`
	AWSRole               string `db:"aws_role"`
	Region                string `db:"region"`
	PulumiStack           string `db:"pulumi_stack"`
	VPCID                 string `db:"vpc_id"`
	EKSInstanceID         uint32 `db:"eks_instance_id"`
	KafkaInstanceID       uint32 `db:"kafka_instance_id"`
	DBInstanceID          uint32 `db:"db_instance_id"`
	MemoryDBInstanceID    uint32 `db:"memory_db_instance_id"`
	ElastiCacheInstanceID uint32 `db:"elasticache_instance_id"`
}

type EKS struct {
	InstanceID   uint32 `db:"instance_id"`
	ClusterID    string `db:"cluster_id"`
	MinInstances uint32 `db:"min_instances"`
	MaxInstances uint32 `db:"max_instances"`
	InstanceType string `db:"instance_type"`
}

type Kafka struct {
	InstanceID            uint32 `db:"instance_id"`
	ConfluentEnvironment  string `db:"confluent_environment"`
	ConfluentClusterID    string `db:"confluent_cluster_id"`
	ConfluentClusterName  string `db:"confluent_cluster_name"`
	KafkaBootstrapServers string `db:"kafka_bootstrap_servers"`
	KafkaAPIKey           string `db:"kafka_api_key"`
	KafkaSecretKey        string `db:"kafka_secret_key"`
}

type DB struct {
	InstanceID           uint32 `db:"instance_id"`
	ClusterID            string `db:"cluster_id"`
	ClusterSecurityGroup string `db:"cluster_security_group"`
	DBHost               string `db:"db_host"`
	AdminUsername        string `db:"admin_username"`
	AdminPassword        string `db:"admin_password"`
}

type MemoryDB struct {
	InstanceID           uint32 `db:"instance_id"`
	ClusterID            string `db:"cluster_id"`
	ClusterSecurityGroup string `db:"cluster_security_group"`
	Hostname             string `db:"hostname"`
}

type ElastiCache struct {
	InstanceID           uint32 `db:"instance_id"`
	ClusterID            string `db:"cluster_id"`
	ClusterSecurityGroup string `db:"cluster_security_group"`
	PrimaryHostname      string `db:"primary_hostname"`
	ReplicaHostname      string `db:"replica_hostname"`
}
