package sagemaker

type Model struct {
	Id               uint32 `db:"id"`
	Name             string `db:"name"`
	Version          string `db:"version"`
	Framework        string `db:"framework"`
	FrameworkVersion string `db:"framework_version"`
	ArtifactPath     string `db:"artifact_path"`
	Active           bool   `db:"active"`
	LastModified     int64  `db:"last_modified"`
}

type SagemakerHostedModel struct {
	SagemakerModelName string `db:"sagemaker_model_name"`
	ModelId            uint32 `db:"model_id"`
	ContainerHostname  string `db:"container_hostname"`
}

type SagemakerEndpointConfig struct {
	Name          string `db:"name"`
	VariantName   string `db:"variant_name"`
	ModelName     string `db:"model_name"`
	InstanceType  string `db:"instance_type"`
	InstanceCount uint   `db:"instance_count"`
}

type SagemakerEndpoint struct {
	Name               string `db:"name"`
	EndpointConfigName string `db:"endpoint_config_name"`
	Active             bool   `db:"active"`
}
